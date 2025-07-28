package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

import com.fasterxml.jackson.core.TreeNode;


/**
 * a request that is executed asynchronously in the client
 *
 * @param <T> the type of value returned by the request
 */
public class JRPCAsyncRequest<T> extends JRPCSimpleRequest<T> {
	private class RequestFuture extends CompletableFuture<T>{
		// so far it appears I do not need requestId at all.  It might be needed if the partner supported a "cancel" request	
		@SuppressWarnings("unused")
		int requestId = -1;	
	}
	private final RequestFuture future;  //can be given a timeout to derive a new CompletableFuture<T>
	/*public JRPCAsyncRequest(ObjectNode request, Class<T> resultClass) {
		super(request, resultClass);
		request.put("asynchronous", true);
		this.future = new CompletableFuture<T>();
	}*/
	public JRPCAsyncRequest (OutboundRequest<T> outbound, JRPCAgent agent, Map<String, TreeNode>meta, String methodName, T resultInstance, 
			Object resultClass, Object parameters) {
		super(outbound, agent, meta, methodName, resultInstance, resultClass, parameters);
		this.future = new RequestFuture();
		/*if (serverAsynchronous && this.request != null)
			request.put("asynchronous", true);*/
	}
	
	@Override
		int submit(Connection conn) throws IOException {
		  return future.requestId = super.submit(conn);}

	@Override 
	boolean isCancelled() {return future.isCancelled();}

    @Override
	public void completed(IncomingMessage /*Map<String, ?>*/ msg) {
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
    
	public CompletableFuture<T>getFuture(){return future;}
	
    T get() throws InterruptedException, ExecutionException {
    	return future.get();}
    
    public static <T1> T1  executeWithTimeout(CompletableFuture<T1> future, long millis) 
    		throws TimeoutException, InterruptedException, ExecutionException{
    	try {
			return future.get(millis, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			future.cancel(false); //that will cause the agent to ignore the eventual result.
			throw e;
		}catch(CancellationException e) {//this is a runtime exception in java
			throw e;
		}/*catch (InterruptedException | ExecutionException  e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}*/
    }
}
