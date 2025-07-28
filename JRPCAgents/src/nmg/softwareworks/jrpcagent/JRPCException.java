package nmg.softwareworks.jrpcagent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * <p>JRPCException is the abstract superclass of Exceptions thrown due to errors in execution of a json rpc request.
 * Such an exception might indicate an error in the request itself, or a problem handling the request.
 * It might also indicate some problem serializing the request or deserializing the result, including
 * the possibility that the deserialized result was not of the expected type.
 * (de)serialization errors generally indicate a problem in the implementation of the jrpc protocol.
 * </p>
 * 
 * @author nmg
 *
 */
public abstract class JRPCException extends Exception implements JRPCProblem{
	private static final int parseErrorCode = -32700;
	private static final int invalidRequestCode = -32600;
	private static final int methodNotFoundCode = -32601;
	private static final int invalidParamsCode = -32602;	
	private static final int internalErrorCode = -32603;  //unclear what the spec means this to be used for!
	private final JsonNode data;
	private final int code;
	public static JRPCException fromError(JRPCSimpleRequest<?> req, ObjectNode error) {
		int code = error.get("code").asInt();
		switch (code) {
			case invalidParamsCode:
				return new InvalidParameters(req, error);
			case methodNotFoundCode:
				return new UnknownMethod(req, error);
			case invalidRequestCode:
				return new InvalidRequest(req, error);
			case parseErrorCode:
				return new ParseError(req, error);
			default:
				return new JRPCApplicationError(error);
		}		
	}
	private JRPCException(ObjectNode error) {
		super(error.get("message").asText());
		this.code = error.get("code").asInt();
		//this.message = error.get("message").asText();
		this.data = error.get("data"); //JsonUtilities.deserialize(error.get("data"), Object.class);		
	}
	protected JRPCException(Throwable t) {
		super(t.getLocalizedMessage());
		this.code = internalErrorCode;
		this.data = null;
	}
	protected JRPCException(String message) {
		this(message, internalErrorCode);}

	protected JRPCException(String message, int code) {
		super(message);
		this.code = code;
		//this.message = message;
		this.data = null;		
	}
	
	/**
	 * @return The deserialized data field of this JRPCException
	 */
	@Override
	public final Object getData() {return data;}
	/**
	 * @return the integer code field of this JRPCException
	 */
	@Override
	public final int getCode() {return code;}
	
	/**
	 * @return the message field of this JRPCException
	 */
	//@Override	public final String getMessage() {return message;}
	
	/**
	 * an UnknownMethod exception is thrown as the response to a request
	 * if the server does not recognize the method name string in a json rpc request.
	 * @author nmg
	 *
	 */
	public static class UnknownMethod extends JRPCException{
		private final String methodName;
		UnknownMethod (JRPCSimpleRequest<?> req, ObjectNode error) {
			super(error);
			this.methodName = req.getMethodName();
		}
		/**
		 * @return the method name that was not recognized by the server
		 */
		public String getMethodName() {return methodName;}
	}

	/**
	 * an InvalidParameters exception is thrown as the response to a request
	 * if the server reports that the parameter value(s) in a json rpc
	 * request are not acceptable for the method of the request.
	 * @author nmg
	 *
	 */
	public static class InvalidParameters extends JRPCException{
		private final JRPCSimpleRequest<?> req;
		InvalidParameters (JRPCSimpleRequest<?> req, ObjectNode error) {
			super(error);
			this.req = req;
		}
		/**
		 * @return the JRPCRequest that contained unacceptable parameters. This request contains
		 * both the name of the method it uses and the supplied parameter values.  
		 */
		public JRPCSimpleRequest<?> getRequest(){return req;}
	}
	
	/**
	 * An InvalidRequest exception is thrown as the response to a request
	 * if the server reports that the request is not valid.  The message and/or data
	 * of the exception should elaborate the reason.  The documentation of requests that
	 * @author nmg
	 *
	 */
	public static class InvalidRequest extends JRPCException{
		private final JRPCSimpleRequest<?> req;
		InvalidRequest (JRPCSimpleRequest<?> req, ObjectNode error) {
			super(error);
			this.req = req;
		}
		/**
		 * @return the JRPCRequest that received this error response
		 */
		public JRPCSimpleRequest<?> getRequest(){return req;}
	}
	
	private static final int generalApplicationErrorCode = 0;
	/**
	 * a JRPCApplicationError is thrown as the response to a request
	 * when the server responds to a request with a custom error code.  The
	 * message and data of the exception should explain the error.
	 *
	 */
	public static class JRPCApplicationError extends JRPCException{
		JRPCApplicationError (ObjectNode error) {
			super(error);	}

		public JRPCApplicationError (Throwable t) {
			super(t.getLocalizedMessage(), generalApplicationErrorCode);	}
	}
	
	public static class JRPCClosedConnectionError extends JRPCException{
		JRPCClosedConnectionError (Connection c) {
			super(String.format("attempt to send a request on closed connection %s", c));	}
	}	
	
	/**
	 * A ParseError is thrown in response to a request 
	 * if the server reports an error using the error code reserved by Json RPC for parsing errors.
	 *
	 */
	public static class ParseError extends JRPCException{
		private final JRPCSimpleRequest<?> req;
		ParseError (JRPCSimpleRequest<?> req, ObjectNode error) {
			super(error);
			this.req = req;
		}
		/**
		 * @return the JRPCRequest that received this error response
		 */
		public JRPCSimpleRequest<?> getRequest() {return req;}
	}

	/**
	 * An InterruptionError is thrown if a connection is closed while a synchronous jrpc call is
	 * waiting for the result to arrive.
	 *
	 */
	public static class InterruptionError extends JRPCException{
		public InterruptionError(InterruptedException e){super(e);}
	}
	
	public static class InternalJRPCException extends JRPCException{
		public InternalJRPCException(String msg){super(msg);}
		public InternalJRPCException(Throwable t) {super(t);}
	}
}
