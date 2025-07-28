package nmg.softwareworks.jrpcagent;


/**
 * A JRPCRuntimeException may be thrown due to problems detected when transmitting a request or processing a response.
 * @author nmg *
 */
public abstract class JRPCRuntimeException extends RuntimeException implements JRPCProblem{

	private final int code;
	@Override
	public int getCode() {return code;}
	@Override
	public Object getData() {return null;}
    //getMessage is handled by RuntimeException superclass
	
	public JRPCRuntimeException(String explanation, int code) {
		this(explanation, code, null);
	}
	JRPCRuntimeException(String explanation, int code, Throwable t) {
		super(explanation, t);
		this.code = code;
	}
	

	private final static int deserializationErrorCode = 50;
	private final static int serializationErrorCode = 51;
	private final static int communicationsErrorCode = 52;
	private final static int parameterErrorCode = 53;
	/**
	 * A DeserializationFailure exception is thrown if a request or response cannot be deserialized.
	 * @author nmg
	 */
	public static class DeserializationFailure extends JRPCRuntimeException{
		public DeserializationFailure(String msg) {	this(msg, null);}
		public DeserializationFailure(String msg, Throwable t) {
			super(msg, deserializationErrorCode, t);	}
	}
	

	private static final int methodNotFoundCode = -32601;
	public static class UnknownMethodFailure extends JRPCRuntimeException{
		public UnknownMethodFailure(String methodName) {
			super(String.format("no registered handler for method [%s]", methodName), methodNotFoundCode);	}
	}
	
	/**
	 * A SerializationFailure exception is thrown if a request or response cannot be serialized.
	 * @author nmg
	 */
	public static class SerializationFailure extends JRPCRuntimeException{
		//private final JRPCRequest<?> request;
		public SerializationFailure (String msg, Throwable cause) {
			super(msg, serializationErrorCode, cause);
			//this.request = request;
		}
		//public JRPCRequest<?> getRequest() {return request;}
	}
	
	/**
	 * A CommunicationsFailure exception is thrown if the communication channel between client and server breaks.
	 * @author nmg
	 */
	public static class CommunicationsFailure extends JRPCRuntimeException{
		//private final JRPCRequest<?> request;
		public CommunicationsFailure (String msg, Throwable cause) {
			super(msg, communicationsErrorCode, cause);
			//this.request = request;
		}
		//public JRPCRequest<?> getRequest() {return request;}
	}
	
	/**
	 * Parameters for requests are understood either 'positionally' or 'by name', depending on the request.
	 * In either case, the supplied parameters may be determined to be invalid.  This exception is thrown
	 * for parameter errors detected by this library before the request is sent to the server. When this
	 * exception is thrown the request is not actually issued.
	 * @author nmg
	 *
	 */
	public static class ActualParameterException extends JRPCRuntimeException{
		//private final ServerRequest<?> request;
		/*ActualParameterException(ServerRequest<?> request) {
			super("attempt to execute a request with unacceptable parameters");
			this.request = request;
		}*/
		public ActualParameterException(String explanation) {
			super(explanation, parameterErrorCode);
			//this.request = request;
		}
		/*
		 * @return the ServerRequest for which supplied parameters were  invalid.
		 */
		//public ServerRequest<?> getRequest() {return request;}
	}
}
