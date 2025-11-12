package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.util.concurrent.*;
//import java.util.Map;
//import com.fasterxml.jackson.core.TreeNode;

import com.fasterxml.jackson.core.type.TypeReference;

import nmg.softwareworks.jrpcagent.JRPCException.JRPCRequestTimeoutException;


/**
 * a request that is executed asynchronously in the client
 *
 * @param <T> the type of value in the result of a response to the request 
 */
public class JRPCAsyncRequest<T> extends JRPCSimpleRequest<T> {
	private class RequestFuture extends CompletableFuture<T>{
		// so far it appears I do not need requestId at all.  It might be needed if the partner supported a "cancel" request	
		@SuppressWarnings("unused")
		int requestId = -1;	
	}
	private final RequestFuture future;  //can be given a timeout to derive a new CompletableFuture<T>

	public JRPCAsyncRequest (OutboundRequest<T> outbound, //JRPCAgent agent, Map<String, TreeNode>meta, String methodName, Object resultClass,
			 Object parameters) {
		super(outbound, /*agent, meta, methodName, resultInstance, resultClass,*/  parameters);
		this.future = new RequestFuture();
	}
	public JRPCAsyncRequest (Class<T> classz, String methodName, Object parameters) {
		//JRPCAgent agent, Map<String, TreeNode>meta, String methodName, Object resultClass, Object parameters) {
		super(true, classz, methodName, parameters);
		this.future = new RequestFuture();
	}
	public JRPCAsyncRequest (TypeReference<T> resultType, String methodName, Object parameters) {
		//JRPCAgent agent, Map<String, TreeNode>meta, String methodName, Object resultClass, Object parameters) {
		super(true, resultType, methodName, parameters);
		this.future = new RequestFuture();
	}
	
	@Override
		int submit(Connection conn) throws IOException {
		  return future.requestId = super.submit(conn);}

	@Override 
	boolean isCancelled() {return future.isCancelled();}

    @Override
	public void completed(IncomingMessage  msg) {
    	if (future.isCancelled()) return;
    	this.response = new JRPCResponse<T>(msg, this);
    	var result = response.getResult();
    	if (result != null)
			try {// could still have deserialization trouble
				future.complete(handleSuccessResponse());
			} catch (RuntimeException e) {
				future.completeExceptionally(e);}
    	else { //error returned
    		future.completeExceptionally(JRPCException.fromError(this, response.getError()));
    	}
    }
    @Override
    void onConnectionClosed() { //called when the connection on which this request is awaiting a response gets closed
    	if (future.isCancelled()) return;
    	future.completeExceptionally(new JRPCException.JRPCApplicationError (new Exception("the connection on which this request's response would be delivered has been closed.")));
	}
    
	public CompletableFuture<T>getFuture(){return future;}
	
    T get() throws InterruptedException, ExecutionException {
    	return future.get();}
    
    public static <T1> T1  executeWithTimeout(OutboundRequest<?> request, CompletableFuture<T1> future, long millis) 
    		throws JRPCRequestTimeoutException, InterruptedException, ExecutionException{
    	try {
			return (T1)future.get(millis, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			future.cancel(false); //that will cause the agent to ignore the eventual result.
			throw new JRPCRequestTimeoutException(request);
		}catch(CancellationException e) {//this is a runtime exception in java
			throw e;
		}/*catch (InterruptedException | ExecutionException  e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}*/
    }
}
