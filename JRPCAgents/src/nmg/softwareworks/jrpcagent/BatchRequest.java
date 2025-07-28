package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

// A BatchRequest holds a collection of JRPCSimpleRequests  that will eventually be transmitted to a partner.
class BatchRequest {	
	
    private final HashMap<Connection, Collection<JRPCSimpleRequest<?>>> outgoingRequests =
    		new HashMap<Connection, Collection<JRPCSimpleRequest<?>>>();
    
    void addPendingRequest(Connection conn, JRPCSimpleRequest<?> request) {
    	var requests = outgoingRequests.get(conn);
    	if (requests == null) {
    		requests = new ArrayList<JRPCSimpleRequest<?>>();
    		outgoingRequests.put(conn, requests);
    	}
    	requests.add(request);
    	//for requests that are not notifications, the id will be assigned when the batch is being sent
    }
    
    boolean isEmpty() {return outgoingRequests.isEmpty();}
    
    void addAll (BatchRequest other) {//add all the entries from other to this batch
    	for (var pair :other.outgoingRequests.entrySet()) {
    		var conn = pair.getKey();
    		var requestsToAdd = pair.getValue();
    		var requests = outgoingRequests.get(conn);
    		if (requests == null)
    			outgoingRequests.put(conn, requestsToAdd);
    		else requests.addAll(requestsToAdd);
    	}    	
    }
    
    void submit() {
    	for (var pair : outgoingRequests.entrySet()) {
    		try {
				pair.getKey().postBatchRequest(pair.getValue());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
    	}    	
    }

}
