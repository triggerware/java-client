package calqlogic.twservercomms;

import java.time.Duration;
import java.util.Collection;
import com.fasterxml.jackson.databind.JavaType;
import nmg.softwareworks.jrpcagent.*;

public abstract class PushResultController<T> implements NotificationInducer{	
	final String notificationMethod = TriggerwareClient.nextNotificationMethod("__rsPush");
	final Class<T>rowType;
	private final int notifyLimit;
	private final Integer rowLimit;
	private final Duration notifyTimeLimit;
	private final boolean closed = false;
    private boolean closeRequested = false;
	private Integer handle = null;
	private TriggerwareConnection connection = null;
	private JavaType notificationType; 
	public abstract void handleResults(Collection<T> rows, boolean exhausted) ;
	public PushResultController(Class<T>rowType, int notifyLimit, Duration notifyTimeLimit, Integer rowLimit) {
		if(notifyLimit<=0) throw new IllegalArgumentException("notifyLimit for a QueryResultController must be positive");
		this.rowType = rowType;
		this.rowLimit = rowLimit;
		this.notifyLimit = notifyLimit;
		this.notifyTimeLimit = notifyTimeLimit;
	}
	Integer getHandle() {return handle;}
	
	NamedRequestParameters getParams() {
		var params = new NamedRequestParameters()
				.with("limit", rowLimit) .with("method", notificationMethod).with("handle", handle)
				.with("notify-limit", notifyLimit);
		if (notifyTimeLimit != null) 
			params.with("notify-timelimit", notifyTimeLimit.toMillis()/1000.);
		return params;
	}

	public Object getNotificationType() {return notificationType;}
	/*private synchronized void ensureFresh() {
		if (handle != null) close();
		closed = closeRequested = false;
		handle = null;
	}*/
	public synchronized void close() {
		if (closed) return;
		if (handle == null) {
			closeRequested = true;
			return;
		}
		closeRequested = false;

		try {TWResultSet.closeResultSetRequest.execute(connection, handle);
		}catch(JRPCException e) {
			Logging.log("error closing a TWResultSet <%s>",e.getMessage());
		}
		connection.getAgent().unregisterNotificationInducer(notificationMethod);
		//connection.unregisterQueryResultController(handle);
		handle = null;
	}
	synchronized void setHandle(TriggerwareConnection connection, int handle) {
		this.handle = handle;
		this.connection = connection;
		if (closeRequested) close();
		else {
			notificationType = connection.getTypeFactory().constructParametricType(QueryPushNotification.class, rowType);
			connection.getAgent().registerNotificationInducer(notificationMethod, this);
			//connection.registerQueryResultController(handle, this);
		}
	}		
}

