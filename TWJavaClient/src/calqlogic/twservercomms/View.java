package calqlogic.twservercomms;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import nmg.softwareworks.jrpcagent.JRPCException;
import nmg.softwareworks.jrpcagent.NamedRequestParameters;

/**
 * A View represents a named virtual table (relation) defined by some sql query/fol description
 * Like other tables/relations, a view belongs to a specific schema.
 * A view's lifetime is the lifetime of the connection on which it is created, unless it is explicitly closed earlier.
 *
 * @param <T> the tuple type for the view
 * 
 * There should be a TW Server configuration ability that supports the definition of views that are not tied to any specific
 * connection and cannot be closed.
 */
public class View<T> extends AbstractQuery<T> implements Statement{
	//private  Class<?>[]  signatureTypes;  
	//private  String[] signatureNames, signatureTypeNames;
	private final String name;

	/*@JsonFormat(shape=JsonFormat.Shape.OBJECT)
	static class ViewRegistration{
		//final int handle;
		final SignatureElement[]signature;
		@JsonCreator
		ViewRegistration( @JsonProperty("signature")SignatureElement[] signature){
			//this.handle = handle;
			this.signature = signature;	
		}

		Class<?>[] typeSignature(){return typeSignatureTypes(signature);}

		String[] typeNames(){return typeSignatureTypeNames(signature);}
	}*/

	private View(TriggerwareClient client, Object tupleType, String name, String query, String language, String schema){
		super(tupleType, query, language, schema);
		this.name = name;
		//crsResultType = parametricTypeFor(client, QueryStatement.ExecuteQueryResult.class);
	}

	/**
	 * create a View and register it for use on a connection
	 * @param tupleType the tuple type  of the view
	 * @param name the name of the view
	 * @param query  the sql/fol query that defines the view
	 * @param language the appropriate members of  {@link Language} for the query
	 * @param schema the schema in which the view will be defined, also the default schema for the query
	 * @param connection the connection on which this prepared query will be reistered and later used
	 * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
	 */
	public View(Object tupleType, String name, String query, String language, String schema, 
						 TriggerwareConnection connection) throws JRPCException{
		this (connection.getClient(), tupleType, name, query, language, schema);
		registerNoCheck(connection);
	}
	
	/**
	 * register a prepared query for use on a connection
	 * @param connection the connection on which the query will be used
	 * @throws JRPCException for problems signaled by the TW Server, such as invalid syntax in the query.
	 */
	public synchronized void register(TriggerwareConnection connection) throws JRPCException {
		if (this.connection != null)
			throw new ReregistrationError();
		registerNoCheck(connection);
	}

	private  void registerNoCheck(TriggerwareConnection connection) throws JRPCException {
		//Object[]params = new Object[] {language, schema, name, query};
		var params = new NamedRequestParameters().with("name", name).with("query", query).with("namespace", schema).with("language", language);
		/*var vresult = (ViewRegistration)*/connection.synchronousRPC(Void/*ViewRegistration*/.class, null, null, "define-view",params);
		registered(/*vresult,*/ connection);
	}

	/**
	 * create a PreparedQuery and register it for use on a client's primary connection
	 * @param tupleType the tuple type of the result of the PreparedQuery
	 * @param name the name of the view
	 * @param query  the sql query containing input placeholders
	 * @param language the appropriate members of  {@link Language} for the query
	 * @param schema the default schema for the query
	 * @param client the client that will use this query on its primary connection
	 * @throws JRPCException if the server refuses the request to create a prepared query for the query/schema values supplied.
	 */
	public View(Object tupleType, String name, String query, String language, String schema, 
			 TriggerwareClient client) throws JRPCException{
		this (tupleType, name, query, language, schema, client.getPrimaryConnection());}

	private void registered(/*ViewRegistration dvResult,*/ TriggerwareConnection connection) {
        //recordRegistration(connection, -1);
		connection.addView(this);
		/*signatureNames = signatureNames(dvResult.signature);
		signatureTypes = dvResult.typeSignature();
		signatureTypeNames = dvResult.typeNames();*/
	}

	/*public String[] getSignatureNames(){return signatureNames;}
	public Class<?>[] getSignatureTypes(){return signatureTypes;}
	public String[] getSignatureTypeNames(){return signatureTypeNames;}*/

	//private static PositionalParameterRequest<Void> releaseViewRequest = 
	//		new PositionalParameterRequest<Void>(Void.TYPE, null, "release-view", 2, 2);

	@Override
	public void close() {closeQuery();}

	@Override
	public boolean closeQuery() {
		if (closed) return false;
		closed = true;
		/*try {
			releaseViewRequest.execute(connection, name, schema);
		} catch (JRPCException e) {
		   Logging.log("error closing a View <%s>", e.getMessage());
		   return false;
		}*/
		connection.removeView(this);
		return true;
	}
}
