package nmg.softwareworks.jrpcagent;

import com.fasterxml.jackson.databind.node.ObjectNode;

class JRPCResponse<T> {
	final Object requestId;
	private final T result;
	private final boolean hasResult;
	private final ObjectNode responseError;

	@SuppressWarnings("unchecked")
	JRPCResponse(IncomingMessage response, JRPCSimpleRequest<T> request) {
		//this.request = request;
		requestId = response.id;
		hasResult = response.hasResult;
		if (hasResult) 
			result = (T)response.result;
		else result = null;
		responseError = response.responseError;
	}
	JRPCResponse(JRPCSimpleRequest<T> request) {//this constructor is used when the connection on which a request is awaiting a response is closed
		//this.request = request;
		requestId = request.requestId;
		hasResult = false;
		result = null;
		responseError = JsonUtilities.jnfactory.objectNode();
		responseError.put("code", -32000);
		responseError.put("message", "connection closed prior to receiving response");
	}
	public T getResult() {return  result;}
	public boolean hasResult() {return hasResult;}

	public ObjectNode getError() {	return  responseError;	}
	//void logError() {
	//	Utilities.log(String.format("error response from request %d: %s", requestId, error));}
}
