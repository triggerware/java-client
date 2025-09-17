package calqlogic.twservercomms;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.List;
import com.fasterxml.jackson.databind.JavaType;

import calqlogic.twservercomms.AbstractQuery.SignatureElement;
import nmg.softwareworks.jrpcagent.*;

/**
 * A NotificationResultController provides information needed to use the option of having query result batches returned as notifications.
 * It supplies parameters sent to the TW server that control when the batches are sent and how many rows will be in a batch.
 * It is also provides the code for handling a batch of rows received in a notfication (via its handleRows method)
 *
 * @param <T> The java type that will be used as the deserialization of each individual row
 */
public abstract class NotificationResultController<T> implements NotificationInducer, Closeable{	
	final String notificationMethod = TriggerwareClient.nextNotificationMethod("__rsPush");
	final Class<T>rowClass;
	private final Integer notifyLimit; //limits the total number of rows
	private final Integer rowLimit; //limits the number of rows in a single notification
	private final Duration notifyTimeLimit;
	private boolean closed = false, closeRequested = false;
	// the remaining fields are set when the controller is used on a connection
	private Integer handle;
	private TriggerwareConnection connection = null;
	private JavaType notificationType;
	private SignatureElement[] rowSignature;
	private Constructor<T>rowConstructor;
	private TWResultSet<T> resultSet;
	/** each concrete subclass must have or inherit an implementation of this method to handle the rows in a notification
	 * @param rows the rows included in a notification.
	 * @param exhausted true if it is known that there will be no more notifications after this one.
	 * @return true to immediately request the TW server to stream additional rows, using the same resource limit parameter that were used to produce the batch just handled.
	 * The return value is ignored if exhausted is true.
	 */
	public abstract boolean handleRows(List<T> rows, boolean exhausted) ;
	public NotificationResultController(Class<T>rowClass, Integer notifyLimit, Duration notifyTimeLimit, Integer rowLimit) {
		if(notifyLimit!=null && notifyLimit<=0) throw new IllegalArgumentException("notifyLimit for a QueryResultController must be positive");
		this.rowClass = rowClass;
		this.rowLimit = rowLimit;
		this.notifyLimit = notifyLimit;
		this.notifyTimeLimit = notifyTimeLimit;
	}
	Integer getHandle() {return handle;}
	NamedRequestParameters controlParams() {
		var params = new NamedRequestParameters()
				.with("limit", rowLimit) .with("method", notificationMethod).with("handle", handle)
				.with("notify-limit", notifyLimit);
		if (notifyTimeLimit != null) 
			params.with("notify-timelimit", notifyTimeLimit.toMillis()/1000.);
		return params;
	}

	public Object getNotificationType() {return notificationType;}

	@Override
	public void establishDeserializationAttributes(SerializationState ss) {
		ss.put("rowSignature", rowSignature);
		ss.put("rowBeanConstructor", rowConstructor);
	}
	TWResultSet<T> getResultSet(){return resultSet;}
	public synchronized void close() {
		if (closed) return;
		if (handle == null) {
			closeRequested = true;
			return;
		}
		closeRequested = false;

		try {TWResultSet.closeResultSetRequest.execute(connection, handle);
		}catch(JRPCException e) {Logging.log("error closing a TWResultSet <%s>",e.getMessage());}
		connection.getAgent().unregisterNotificationInducer(notificationMethod);
		handle = null;
	}

	synchronized void setHandle(TriggerwareConnection connection, int handle, SignatureElement[] rowSignature, Constructor<T> rowConstructor,
			                       TWResultSet<T>resultSet) {
		this.handle = handle;
		this.connection = connection;
		this.rowSignature = rowSignature;
		this.rowConstructor = rowConstructor;
		this.notificationType = connection.getTypeFactory().constructParametricType(QueryPushNotification.class, rowClass);
		this.resultSet = resultSet;
		if (closeRequested) close();
		else {
			connection.getAgent().registerNotificationInducer(notificationMethod, this);
		}
	}		
}

