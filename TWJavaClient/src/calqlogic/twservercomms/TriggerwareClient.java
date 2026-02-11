package calqlogic.twservercomms;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import nmg.softwareworks.jrpcagent.*;
import nmg.softwareworks.jrpcagent.JRPCException.JRPCClosedConnectionError;


/**
 *<p>A TriggerwareClient is a {@link JRPCAgent} that provides one or more connections to a single Triggerware server.
 *The TriggerwareClient constructor establishes an initial ('primary') connection to the TW server, whose identity is
 *determined by host/port parameters of the constructor. This connection is special only in that many APIs in this library
 *having a TriggerwareClient parameter need to access one of its connections.  These APIs use the primary connection.
 *</p><p>
 *TriggerwareClient contains methods for issuing  specific requests that are supported by any Triggerware server.
 *Classes that extend TriggerwareClient for specific applications will implement their own application-specific
 *methods to make requests that are idiosyncratic to a Triggerware server for that application.  Any such requests must be
 *implemented in lisp (the server's implementation langauge) and included in the server -- they cannot be added by a client.
 *</p><p>
 *A TriggerwareClient can also manage {@link Subscription Subscriptions}. By subscribing to certain kinds of changes, the
 *client arranges to be notified when these changes occur in the data accessible to the server.  This capability also relies
 *on natively implemented code that must be added to a Triggerware server.
 *</p>
 */
public class TriggerwareClient extends JRPCAgent{
	/*static JsonMapper twMapper() {
		var mapper = jsonMapper(new HashMap<String,Object>());
		TemporalSerialization.configureForMapper(mapper);
		//TWJson.configureForMapper(mapper);
		mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		return mapper;
	}*/
	private static SSLSocketFactory sslSocketFactory =
            (SSLSocketFactory)SSLSocketFactory.getDefault();
	
	private Integer defaultFetchSize = 10;

	//final static Map<String, TreeNode> serverAsynchronousMap = new HashMap<String, TreeNode>(1);
	//static {serverAsynchronousMap.put("asynchronous", BooleanNode.valueOf(true));}

	/**
	 * tagCounter is used by Subscriptions and PolledQueries to ensure unique tags in notifications
	 */
	private static AtomicInteger methodCounter = new AtomicInteger(1);
	static String nextNotificationMethod(String prefix) {
		return prefix + methodCounter.getAndIncrement();}
	
	private String defaultSchema = null;
	/**
	 * @return the default schema that will be used in connections created by this client
	 */
	String getDefaultSchema() {return defaultSchema;}
	
	/**
	 * @param defaultSchema the default schema in connections created by this client
	 */
	void setDefaultSchema(String defaultSchema) {this.defaultSchema = defaultSchema;}
	

	/**
	 * @return the default fetch size for TriggerwareClients
	 */
	public Integer getDefaultFetchSize() {return defaultFetchSize;}
	/**
	 * @param size  determines the intial fetch size for a result set returned by requests that query
	 * the TW server for data, unless overriden on the request. The size can an integer. It can also be null, which means that 
	 * <em>all</em> results should be returned in the resultset.
	 * @return the previous default fetch size
	 */
	public Integer setDefaultFetchSize(Integer size) {
		var old = defaultFetchSize;
		defaultFetchSize = size;
		return old;
	}
	/**
	 * TriggerwareClientException is the root class for exceptions that might be thrown by a TriggerwareClient
	 * as a result of issuing a request to the server or handling a notification from the server.
	 * A TriggerwareClientException is <em>not</em> a problem reported by the TW server.
	 */
	public static class TriggerwareClientException extends Exception{ //should be abstract class, invent subclasses for actual errors
		protected TriggerwareClientException(String msg) {	super(msg);	}
		protected TriggerwareClientException(String msg, Throwable cause) {	super(msg, cause);	}
	}

	/*public static class TimeoutException extends TriggerwareClientException{
		protected TimeoutException(String msg) {super(msg);	}
	}*/
	
	private final int twServerPort;
	private final InetAddress twHost;
	private final boolean useSSL;
	
	/**
	 * @return the network address used by this TriggerwareClient to connect to a server.
	 */
	public InetSocketAddress getServerAddress() {return new InetSocketAddress(twHost, twServerPort);}
	
	/**
	 * create a new client and establish an initial connection to the TW server
	 * @param name is a name for this client.  The name is used in log file entries. 
	 * @param twHost the host where the TW server is listening. Null may be used to designate the loopback host.
	 * @param twServerPort the port number where the TW server is listening.
	 * @throws IOException when a problem occurs establishing a connection to the designated host and port
	 */
	public TriggerwareClient(String name, InetAddress twHost, int twServerPort) throws IOException {
		this(name, twHost, twServerPort, false);}
	
	static final SSLSocket createSSLSocket(InetAddress host, int port) throws  IOException {
		var sock = (SSLSocket)sslSocketFactory.createSocket(host, port);
		sock.setUseClientMode(true);
		sock.startHandshake();
		return sock;
	}
	/**
	 * create a new client and establish an initial connection to the TW server
	 * @param name is a name for this client.  The name is used in log file entries. 
	 * @param twHost the host where the TW server is listening. Null may be used to designate the loopback host.
	 * @param twServerPort the port number where the TW server is listening.
	 * @param useSSL make a connection that uses SSL for communicating over the socket. This requires that the TW server listening
	 * at the designated host/port expects encrypted traffic.
	 * @throws IOException when a problem occurs establishing a connection to the designated host and port
	 */
	public TriggerwareClient(String name, InetAddress twHost, int twServerPort, boolean useSSL) 
			throws IOException {
		super(useSSL ? createSSLSocket(twHost, twServerPort) : new Socket(twHost, twServerPort), name);
		this.twHost = twHost;
		this.twServerPort = twServerPort;
		this.addOutboundProperties("asynchronous");
		//this.setName(name);
		this.useSSL = useSSL;
		establishTWCommunications();
		//getObjectMapper().registerModule(new BatchNotification.DeserializationModule(this));
	}
	
	/**
	 * @return the primary connection of this TriggerwareClient
	 */
	public TriggerwareConnection getPrimaryConnection () { return (TriggerwareConnection) primaryConnection; }

	private boolean tWCommsInitialized = false;
	private synchronized void establishTWCommunications() throws IOException  {
		if (tWCommsInitialized )return ;
		try {
			setSqlDefaults(null, "case-insensitive");
		} catch (JRPCException e) {
			Logging.log(e, "failed to set sql case mode");}		 
		tWCommsInitialized = true;
	}
	
	/**
	 * react to the loss of communications with the Triggerware server on a connection.
	 * The method defined for TriggerwareClient does nothing.
	 * Override this method in a subclass to take some action when communications are lost.
	 * Invocation of this method could occur on the thread dedicated to handling input from the server on connection.
	 * Invocation of this method could also occur on a thread issuing a request to the server.  In the latter case,
	 * the method issuing the request will throw a JRPCRuntimeException.CommunicationsFailure exception.
	 * That exception will be thrown <em>after</em> onTWCommunicationsLost has been invoked. 
	 * This method is <em>not</em> invoked when connections are closed by the client via shutdownTWCommunications.
	 * @param connection -- the connection which is now closed.
	 */
	protected void onTWCommunicationsLost(Connection connection) {};
	
	/**
	 * @return <code>true</code> if communications with the TW server have been established, <code>false</code> otherwise.
	 */
	public boolean isTWCommunicationsInitialized() {return tWCommsInitialized;}

	/**
	 * @return a newly created connection to this client's TW server
	 * @throws IOException if a new connection cannot be established
	 */
	protected synchronized TriggerwareConnection newConnection() throws IOException  {
		return new TriggerwareConnection(this, useSSL ? createSSLSocket(twHost, twServerPort)
				: new Socket(twHost,twServerPort));}
	
	@Override
	public Connection connectToPartner(InputStream istream, OutputStream ostream) throws IOException {
		return new TriggerwareConnection(this,  istream, ostream);}

	JsonMapper getObjectMapper(Connection conn) {return conn.getPartnerMapper();}

	/**
	 * Activate a subscription for individual notifications on this client's primary connection
	 * @param subscription the subscription to activate
	 * @throws SubscriptionException if the client
	 * <ul>
	 * <li> already has this subscription active on a different connection</li>
	 * <li> is requesting batch notifications when already subscribed for individual notifications</li>
	 * <li> is requesting individual notifications when already subscribed for batch notifications.</li>
	 * <li> is requesting a change to the batch group notification name of the active subscription.</li>
	 * </ul>
	 * @throws JRPCException  if the subscription fails for any reason
	 */
	/*@Deprecated
	public void activate(Subscription<?>subscription) throws JRPCException, SubscriptionException {
		subscription.activate(getPrimaryConnection());}*/
	
	/**
	 * Activate a subscription for individual notifications on one of this client's connections
	 * @param subscription the subscription to activate
	 * @param connection  the connection on which notifications will arrive
	 * @throws SubscriptionException if the client
	 * <ul>
	 * <li> already has this subscription active on a different connection</li>
	 * <li> is requesting batch notifications when already subscribed for individual notifications</li>
	 * <li> is requesting individual notifications when already subscribed for batch notifications.</li>
	 * <li> is requesting a change to the batch group notification name of the active subscription.</li>
	 * </ul>
	 * @throws JRPCException  if the server rejects the subscription  for any reason
	 * @throws InvalidConnectionException if the connection does not belong to this client
	 */
	/*@Deprecated
	public void activate(Subscription<?>subscription, TriggerwareConnection connection) throws JRPCException, SubscriptionException, InvalidConnectionException {
		checkConnectionValid(connection);
		subscription.activate(connection);}*/
	
	/**
	 * Activate any currently inactive subscriptions in a BatchSubscription.
	 * If the BatchSubscription contains no active subscriptions, it is set  
	 * @throws SubscriptionException if the client
	 * <ul>
	 * <li> already has this subscription active on a different connection</li>
	 * <li> is requesting batch notifications when already subscribed for individual notifications</li>
	 * <li> is requesting individual notifications when already subscribed for batch notifications.</li>
	 * <li> is requesting a change to the batch group notification name of the active subscription.</li>
	 * </ul>
	 * @throws JRPCException  if the server rejects the subscription  for any reason
	 * @throws InvalidConnectionException if the connection does not belong to this client
	 */
	/*public void activate(BatchSubscription subscription) throws JRPCException, SubscriptionException {
		subscription.activate(getPrimaryConnection());}*/
	
	/**
	 * Activate a subscription for batch notifications on one of this client's connections
	 * @throws SubscriptionException if the client
	 * <ul>
	 * <li> already has this subscription active on a different connection</li>
	 * <li> is requesting batch notifications when already subscribed for individual notifications</li>
	 * <li> is requesting individual notifications when already subscribed for batch notifications.</li>
	 * <li> is requesting a change to the batch group notification name of the active subscription.</li>
	 * </ul>
	 * @throws JRPCException  if the server rejects the subscription  for any reason
	 * @throws InvalidConnectionException if the connection does not belong to this client
	 */
	/*public void activate(BatchSubscription subscription, TriggerwareConnection connection) throws JRPCException, SubscriptionException, InvalidConnectionException {
		checkConnectionValid(connection);
		subscription.activate(connection);
	}*/

	/**
	 * TWRuntimeMeasure allows a client to perform limited measurement of time/space performance in the TW server
	 */
	@JsonFormat(shape=JsonFormat.Shape.ARRAY)
	@JsonPropertyOrder({ "runTime",  "gcTime", "bytes" }) 
	public static class TWRuntimeMeasure{
		public long runTime;
		public long gcTime;
		public long bytes;
	}
	final double serverTimeUnitsPerSecond = Math.pow(10, 6); // that is for a linux clisp twserver
	private static PositionalParameterRequest<TWRuntimeMeasure> runtimeRequest = 
			new PositionalParameterRequest<TWRuntimeMeasure>(TWRuntimeMeasure.class,  "runtime", 0, 0);
	/**
	 * Issue a request on this client's primary connection to obtain a time/space consumption measurement from the TW server.
	 * @return the current TWRuntimeMeasure reported by the TW server.
	 * @throws JRPCException if the server rejects the request
	 */
	public TWRuntimeMeasure runtime() throws JRPCException { return runtimeRequest.execute(primaryConnection);}
	/*public TWRuntimeMeasure runtime(Connection c) throws JRPCException { 
		c = checkConnectionValid(c);
		return runtimeRequest.execute(c);
	}*/
	

	static NamedParameterRequest<Void> setSqlDefaultsRequest =
			new NamedParameterRequest<Void>(Void.TYPE,  "set-global-default",
					null, new String[] {"language", "sql-mode", "sql-namespace"});
	public void setSqlDefaults(String schema, String mode) throws JRPCException {
		var params = new NamedRequestParameters();
		if(mode != null) params.with("sql-mode", mode);
		if (schema != null) params.with("sql-namespace", schema);
		setSqlDefaultsRequest.execute(primaryConnection, params);}
	
	//Movedto SCM
	/*static PositionalParameterRequest<String> validationRequest = 
			new PositionalParameterRequest<String>(String.class,  "validate", 4, 4);
	
	public String validate(String query, String lang, String schema, boolean twoState) throws JRPCException {
		return validationRequest.execute(primaryConnection, query, lang, schema, twoState);	}
	
	
	static NamedParameterRequest<String> paraphraseRequest = 
			new NamedParameterRequest<String>(String.class,  "paraphrase", new String[] {"query"}, new String[] {"lang", "schema"});
	public String paraphrase(String query,  String schema) throws JRPCException {
		var params = new NamedRequestParameters();
		params.with("query", query);
		params.with("lang", "sql");
		if (schema != null) params.with("schema", schema);
		return 	paraphraseRequest.execute(primaryConnection, params);
	}*/
	
	//TODO: do not use the "askquery" server api
	// reimplement this to use QueryStatement
	
	/*
	@SuppressWarnings("rawtypes")
	private static NamedParameterRequest<QueryResponse> askQueryRequest = 
			new NamedParameterRequest<QueryResponse>(QueryResponse.class, false, "askquery", 
					new String[] {"query", "lang", "schema"}, 
					new String[] {"timelimit", "resultlimit", "url-accesslimit", "url-cached-limit", "cachelimits", "progress",
							      "twoState"});
	*/


	static NamedParameterRequest<String> deletePolledQueryRequest = 
			new NamedParameterRequest<String>(String.class,  "delete-polled-query", null,  null);
	//public String deletePolledQuery (NamedRequestParameters parms) throws JRPCException {
	//	return deletePolledQueryRequest.execute(primaryConnection, parms);	}

	
	/**
	 * create a QueryStatement for use on this client's primary connection
	 * @return a new QueryStatement
	 */
	public QueryStatement createQuery() {
		return new QueryStatement((TriggerwareConnection)this.primaryConnection);	}
	
	/**
	 * create a QueryStatement 
	 * @param connection the connection to use for queries issued using the QueryStatement
	 * @return a new QueryStatement
	*/
	public QueryStatement createQuery(TriggerwareConnection connection){
		return new QueryStatement(connection);	}

	public <T> T synchronousRPCP(Class<T>classz,  String method, Object ...params) throws  JRPCException {
		return synchronousRPCP(primaryConnection, classz,   method, params);}
	
	public <T> T synchronousRPCP(Connection connection, Class<T>classz, String method, Object ...params) throws  JRPCException {
		return connection.synchronousRPC(classz,  method, params);}
	/*public <T> T synchronousRPCP(Connection connection, Class<T>classz,   T resultInstance, boolean serverAsynchronous, 
			String method, Object ...params) 	throws  JRPCException {
		//var c = checkConnectionValid(connection);
		return connection.synchronousRPC(null, classz,  resultInstance, serverAsynchronous?serverAsynchronousMap : null, method, params);}*/
		

	public <T> T synchronousRPCN(Class<T>classz, String method, NamedRequestParameters params) throws JRPCException {
		return synchronousRPCN(primaryConnection, classz,   method, params);	}
	/*public <T> T synchronousRPCN(Connection conn, Class<T>classz,  String method, NamedRequestParameters params) throws JRPCException {
		return conn.synchronousRPC(classz, method, params);	}*/
	/**
	 * Issue a client-synchronous request to this client's TW server, providing all the data needed to issue a request.
	 * @param <T> the result type
	 * @param connection the connection on which to issue the request
	 * @param method the json rpc method name for the request
	 * @param classz the result type
	 * @param params the parameters for the request
	 * @return the result of the request
	 * @throws JRPCException if the server responds with an exception
	 */
	public <T> T synchronousRPCN(Connection connection, Class<T>classz, String method, 
			NamedRequestParameters params) throws JRPCException {
		return connection.synchronousRPC(classz,  method, params);	}
	/*public <T> T synchronousRPCN(Connection connection, Class<T>classz, T resultInstance, boolean serverAsynchronous, 
			String method, NamedRequestParameters params) throws JRPCException {
		//var c = checkConnectionValid(connection);
		return connection.synchronousRPC(null,classz, resultInstance, serverAsynchronous?serverAsynchronousMap : null, method, params);	}*/
	
	/**
	 * Issue a client-asynchronous, server-synchronous request to this clients TW server using this client's primary connection, 
	 * creating a new object for the result.
	 * This is the same as
	 * asynchronousRPCP(method, classz,	null, params)
	 * @param <T> the type of the result from this request
	 * @param method the json rpc method name for the request
	 * @param classz the result type
	 * @param params the parameters for the request
	 * @return  a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if this client's primary connection is closed
	 */
	public <T> CompletableFuture<T> asynchronousRPCP(String method, Class<T>classz, Object ...params) throws JRPCClosedConnectionError  {
		//var jrpcRequest = (JRPCAsyncRequest<T>)createRequestP(this, true, false, classz, null, method, params);
		//return primaryConnection.asynchronousRPC(jrpcRequest);
		return asynchronousRPCP(method, classz, null, params);
	}

	/**
	 * Issue a client-asynchronous, server-synchronous request to this client's TW server using this client's primary connection.
	 * @param <T> the type of the result from this request 
	 * @param method the json rpc method name for the request
	 * @param resultType the result type
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if this clients primary connection is closed
	 */
	/*public <T> CompletableFuture<T> asynchronousRPCP(String method, Class<T>classz, T resultInstance, Object ...params) 
			throws JRPCClosedConnectionError {
		var jrpcRequest = (JRPCAsyncRequest<T>)createRequest(this, true, null, classz, resultInstance, method, params);
		return primaryConnection.asynchronousRPC(jrpcRequest);
	}*/
	public <T> CompletableFuture<T> asynchronousRPCP(String method, TypeReference<T>resultType, Object ...params)
			throws JRPCClosedConnectionError {
		var jrpcRequest = //(JRPCAsyncRequest<T>)createRequest(this, true, resultType, method, params);
					      new JRPCAsyncRequest<T>(resultType, method, params);
		return primaryConnection.asynchronousRPC(jrpcRequest);
	}
	/**
	 * Issue a client-asynchronous request to this clients TW server, creating a new object for the result.
	 * This is the same as
	 * asynchronousRPCP(connection,  method, classz, params)
	 * @param <T> the type of the result from this request
	 * @param connection the connection on which to issue the request
	 * @param method the json rpc method name for the request
	 * @param classz the result type
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if the connection is closed
	 */
	/*public <T> CompletableFuture<T> asynchronousRPCP(Connection connection,  String method, Class<T>classz, Object ...params) 
			throws JRPCClosedConnectionError {
		return asynchronousRPCP(connection,  method, classz, null, params);}*/

	/**
	 * Issue a client-asynchronous request to this clients TW server, providing all the data needed to issue a request.
	 * @param <T> the result type
	 * @param connection the connection on which to issue the request
	* @param method the json rpc method name for the request
	 * @param classz the result type
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if the connection is closed
	 */
	public <T> CompletableFuture<T> asynchronousRPCP(Connection connection, String method, Class<T>classz, Object ...params) throws JRPCClosedConnectionError {
		var jrpcRequest = new JRPCAsyncRequest<T>(classz, method, params); //(JRPCAsyncRequest<T>)createRequest(this, true,   classz,  method, params);
		return connection.asynchronousRPC(jrpcRequest);
	}

	/**
	 * Issue a client-asynchronous request to this clients TW server, providing all the data needed to issue a request.
	 * @param <T> the result type
	 * @param connection the connection on which to issue the request
	 * @param method the json rpc method name for the request
	 * @param resultType the result type
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if the connection is closed
	 */
	public <T> CompletableFuture<T> asynchronousRPCP(Connection connection, String method, 
			TypeReference<T>resultType, /*T resultInstance,*/ Object ...params) throws JRPCClosedConnectionError {
		var jrpcRequest = //(JRPCAsyncRequest<T>)createRequest( this, true,  resultType,  method, params);
						  new JRPCAsyncRequest<T>(resultType, method, params);
		return connection.asynchronousRPC(jrpcRequest);
	}

	/**
	 * Issue a client-asynchronous/server-asynchronous request to this clients TW server using this client's primary connection.
	 * A new object will be allocated to hold the result.
	 * This method is the same as calling
	 *   asynchronousRPCN(method, classz, null, params)
	 * @param <T> the result type
	 * @param method the json rpc method name for the request
	 * @param classz the result type
	 * @param params the parameters for the request
	 * @return  a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if this client's primary connection is closed
	 */
	public <T> CompletableFuture<T> asynchronousRPCN(String method, Class<T>classz, NamedRequestParameters params) throws JRPCClosedConnectionError {
		return asynchronousRPCN(method, classz,  params);}

	/**
	 * Issue a client-asynchronous/server-asynchronous request to this clients TW server. Use this client's primary connection.
	 * The request will be issued on this client's primary connection.
	 * @param <T> the result type
	 * @param method the json rpc method name for the request
	 * @param classz the result type
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if this client's primary connection is closed
	 */
	/*public <T> CompletableFuture<T> asynchronousRPCN(String method, Class<T>classz,  T resultInstance,
			NamedRequestParameters params) throws JRPCClosedConnectionError {
		var jrpcRequest = (JRPCAsyncRequest<T>)createRequest(this, true, serverAsynchronousMap, classz,  resultInstance, method, params);
		return primaryConnection.asynchronousRPC(jrpcRequest);
	}*/
	/**
	 * Issue a client-asynchronous/server-asynchronous request to this clients TW server. Use this client's primary connection.
	 * The request will be issued on this client's primary connection.
	 * @param <T> the result type
	 * @param method the json rpc method name for the request
	 * @param resultType the result type
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if this client's primary connection is closed
	 */
	public <T> CompletableFuture<T> asynchronousRPCN(String method, TypeReference<T>resultType, //T resultInstance,
			NamedRequestParameters params) throws JRPCClosedConnectionError {
		var jrpcRequest = //(JRPCAsyncRequest<T>)createRequest(resultType, method, params);
						  new JRPCAsyncRequest<T>(resultType, method, params);
		return primaryConnection.asynchronousRPC(jrpcRequest);
	}

	/**
	 * Issue a client-asynchronous request to this clients TW server.  A new object will be allocated to hold the result.
	 * This method is the same as calling
	 *   asynchronousRPCN(connection, serverAsynchronous, method, classz, null, params)
	 * @param <T> the result type
	 * @param connection the connection on which to issue the request
	 * @param serverAsynchronous <code>true</code> if the request should be handled asynchronously on the server,
	 * otherwise <code>false</code>
	 * @param method the json rpc method name for the request
	 * @param classz the result type
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCException if the server responds with an exception
	 */
	public <T> CompletableFuture<T> asynchronousRPCN(Connection connection, boolean serverAsynchronous, String method,
			Class<T>classz, NamedRequestParameters params) 	throws JRPCException {
		return asynchronousRPCN(connection, serverAsynchronous, method, classz,  params);}

	/**
	 * Issue a client-asynchronous request to this clients TW server, providing all the data needed to issue a request.
	 * @param <T> the request's result type
	 * @param connection the connection on which to issue the request
	 * @param serverAsynchronous <code>true</code> if the request should be handled asynchronously on the server,
	 * otherwise <code>false</code>
	 * @param method the json rpc method name for the request
	 * @param classz the result type for this request
	 * @param resultInstance an instance of the result type for deserializing into, or null to create a new instance
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if the connection is closed
	 */
	/*public <T> CompletableFuture<T> asynchronousRPCN(Connection connection, boolean serverAsynchronous, String method, Class<T>classz, 
			 T resultInstance,  NamedRequestParameters params) throws  JRPCClosedConnectionError {
		//checkConnectionValid(connection);
		var jrpcRequest = (JRPCAsyncRequest<T>)createRequest( this, true, serverAsynchronous?serverAsynchronousMap:null,  classz,
				resultInstance, method, params);
		return connection.asynchronousRPC(jrpcRequest);
	}*/
	/**
	 * Issue a client-asynchronous request to this clients TW server, providing all the data needed to issue a request.
	 * @param <T> the request's result type
	 * @param connection the connection on which to issue the request
	 * @param method the json rpc method name for the request
	 * @param resultType the result type for this request
	 * @param params the parameters for the request
	 * @return a future for obtaining result of the request
	 * @throws JRPCClosedConnectionError if the connection is closed
	 */
	public <T> CompletableFuture<T> asynchronousRPCN(Connection connection, //boolean serverAsynchronous,  
			TypeReference<T>resultType, String method, NamedRequestParameters params) throws JRPCClosedConnectionError {
		//checkConnectionValid(connection);
		var jrpcRequest = //(JRPCAsyncRequest<T>)createRequest(this, true,  resultType, method, params);
						  new JRPCAsyncRequest<T>(resultType, method, params);
		return connection.asynchronousRPC(jrpcRequest);
	}

	@SuppressWarnings("unused")
	private static boolean isBatchNotification(IncomingMessage message) {// called when NOT a request
		var params =  (ObjectNode)message.getPositionalParams()[0];
		return params!=null && params.get("matches")!=null;// will return false unless params is a treenode
	}
}
