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
	public T getResult() {return  result;}
	public boolean hasResult() {return hasResult;}

	public ObjectNode getError() {	return  responseError;	}
	//void logError() {
	//	Utilities.log(String.format("error response from request %d: %s", requestId, error));}
}
