package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.node.*;

import nmg.softwareworks.jrpcagent.JsonUtilities.JRPCObjectMapper;

class JRPCStreamParser implements AutoCloseable {
	private final JRPCObjectMapper inputMapper;
	private final JsonParser jParser;
	private final Connection conn;
	private final JRPCAgent agent;
	private final LoggingReader lr;
	public JRPCStreamParser(Connection conn) throws IOException {
		//super(conn.getInputStream());
		this.conn = conn; 
		inputMapper = conn.getObjectMapper();
		agent = conn.getAgent();
		jParser = JsonUtilities.createStreamDeserializer(conn.getInputStream(), inputMapper);
		lr = (LoggingReader)jParser.getInputSource();
	}

	public void startLogging() {
		lr.setLogging(true);}
	public String logEntryComplete() { 
		return lr.getLoggedText(true);}
	
	private void handleUnexpected(IncomingMessage message, String fieldname) throws IOException {
		handleUnexpected(message,fieldname,false);}
	private void handleUnexpected(IncomingMessage message, String fieldname, boolean unexpectedField) throws IOException {
		//stream has been advanced by nextToken()
		message.ensureUnexpected();
		var tnode = jParser.readValueAsTree();
		Logging.log("unexpected %s %s:<%s>", unexpectedField? "attribute" : "value", fieldname, tnode);
		message.unexpected.put(fieldname, tnode);			
	}

	private IncomingBatch parseOneBatchMessage() {
		var batch = new IncomingBatch(conn);
		//TODO: parse individual requests/notifications and add them to batch
		return batch;
	}
	private IncomingMessage parseOneSimpleMessage() throws IOException {
		var message = new IncomingMessage(conn);				
		JsonToken token;
		while (true) {//parse individual fields
			var propertyName = jParser.nextFieldName();
			if (propertyName==null) { 
				token = jParser.getCurrentToken();
				if (token == JsonToken.END_OBJECT) break;
				Logging.log("unexpected end of message on input stream");
				return message;
			}
			switch(propertyName) {
				case "jsonrpc":
					if (jParser.nextToken() == JsonToken.VALUE_STRING && message.jsonrpc == null)
						message.jsonrpc = jParser.getText();
					else handleUnexpected(message, propertyName);
					break;
				/*case "asynchronous" : {
					var tknas = jParser.nextToken();
					if (tknas == JsonToken.VALUE_FALSE  || tknas == JsonToken.VALUE_TRUE) {
						message.asynchronous = jParser.getBooleanValue();
					} else handleUnexpected(message, fieldname);
					break;
				}*/
				case "id" : {//could be a request or a response. Not a notification
						var firstId = true;
						jParser.nextToken();
						if (message.id != null) {
							firstId = false;
							Logging.log("Multiple id fields in a single message.");
						    handleUnexpected(message,propertyName);
						} else	if (jParser.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) {
							message.id = jParser.getIntValue();
							//if	(conn instanceof Connection) 
								//message.request =((Connection)conn).pendingRequest(message.id);
						} else if (jParser.getCurrentToken() == JsonToken.VALUE_STRING) {
							message.id = jParser.getText();
						} else {
							firstId = false;
							handleUnexpected(message, propertyName);
						}
						if (firstId && conn instanceof Connection) 
							message.request = conn.pendingRequest(message.id);
					}
					break;
				case "result" : // should only occur on a response
					jParser.nextToken();
					Object rslt;
					if (message.request == null) rslt = jParser.readValueAsTree();
					else
						try {
							rslt = message.request.deserializeResult(jParser, conn);
							//Logging.log("have result = [%s,%s]", rslt, rslt!=null?rslt.getClass():"no result");
						} catch (JRPCRuntimeException rte) {
							Logging.log(rte, "while deserializing result");
							message.resultDeserialingError = rte;
							//break;
							throw rte;
						}
					message.setResult(rslt);
					//Logging.log("have result [%s]", rslt);
					break;
				case "error" : // should only occur on a response
					if (jParser.nextToken() == JsonToken.START_OBJECT && message.result == null) {
						message.responseError = jParser.readValueAsTree();
						Logging.log("have error[%s]", message.responseError);
					}else handleUnexpected(message,propertyName);
					break;
				case "method" : // could be a request or a notification
					if (jParser.nextToken() == JsonToken.VALUE_STRING)
						message.methodName =  jParser.getText();
					else handleUnexpected(message, propertyName);
					//Logging.log("have method field");
					break;
				case "params" : // could be a request or a notification
					//Logging.log("processing params field");
					token = jParser.nextToken();
					// example params is [{"a":33,"b":44}]
					if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
						if (message.methodName != null){//this is a request or notification
							//Logging.log("params is for a request or notification");
							try {
								if (message.id != null) // a request
									message.deserializeRequestParams(jParser);
								else {// a notification
									message.deserializeNotificationParams(jParser, inputMapper); }
							}catch (JRPCRuntimeException rte) {
								Logging.log(rte, "while deserializing params");
								message.paramsDeserializingError = rte;
								//break;
								throw rte;
							}
						}  else	message.invalidParams = jParser.readValueAsTree();
						
					}
					else handleUnexpected(message, propertyName);
					//Logging.log("have params field");
					break;
				default:
					token = jParser.nextToken();
					if (agent.allowsInboundProperty(propertyName)) {
						message.addMetaProperty(propertyName, jParser.readValueAsTree());
					} else {
						handleUnexpected(message, propertyName,true);
					}
					break;
			}
		}
		return message;
	}
	IncomingJRPCMessage next() throws IOException {
		// the stream may contain notifications, responses, and requests
		var tkn = jParser.nextToken();
	    if (tkn == null)
	    	throw new java.net.SocketException("null token indicates connection closed");
	    switch(tkn) {
	    	case START_OBJECT:
	    		return parseOneSimpleMessage();
	    	case START_ARRAY:
	    		return parseOneBatchMessage();
	    	default:
				var tnode = jParser.readValueAsTree();
				Logging.log("unexpected message from server starting with token %s: <%s>", tkn, tnode);
				var message = new IncomingMessage(conn);
				message.top = tnode;
				return message;
	    }
	}
	
	public static Object deserializeResult(JsonParser jParser, Class<?>resultClass, Object instanceForResult) throws IOException{
		//Logging.log("deserializeResult class = %s",resultClass);
		if (/*Void.class == resultClass ||*/ Void.TYPE == resultClass) {
			return jParser.readValueAsTree(); // the result will be ignored  and is unconstrained. IT MUST NOT BE Java NULL
		}
		if (TreeNode.class.isAssignableFrom(resultClass)){
				var parsed = jParser.readValueAsTree();
				if (resultClass.isInstance(parsed))
					return parsed;
				else {
					Logging.log("request result type mismatch: expected %s received %s", resultClass, parsed.getClass());
					return null;
				}
		} else {
			if (instanceForResult == null) {
				//var jt = typeFactory.constructType(resultClass);
				return jParser.readValueAs(resultClass);}
			var jrpcr = (JRPCReader)jParser.getInputSource();
			return jrpcr.deserializeIntoObject(instanceForResult, jParser);
		}
	}
	
	public static Object deserializeParameter(JsonParser jParser, Class<?>paramsClass) throws IOException  {
		if (Void.class == paramsClass || Void.TYPE == paramsClass) {
			var params = jParser.readValueAsTree();
			//Logging.log("params for void method are %s",params);
			if (params instanceof ArrayNode && params.size()==0)	return params;
			if (params instanceof ObjectNode && params.size()==0)	return params;
			//Logging.log("wrong params for a void method [%s]", params);
			throw new JRPCRuntimeException.ActualParameterException("supplied parameters for a method with void parameters");
		}
		if (TreeNode.class.isAssignableFrom(paramsClass)){
				var parsed = jParser.readValueAsTree();
				if (paramsClass.isInstance(parsed))
					return parsed;
				else {	 //TODO: log an error
					return null;}
		} else 	return jParser.readValueAs(paramsClass);
	}

	@Override
	public void close() throws Exception {	jParser.close();}

/*	static Object[] parseTuple(JsonParser jsonParser, Class<?>[] sig) throws IOException {
    	//var tkn = jsonParser.currentToken(); //curentToken is START_ARRAY
    	return (sig == null) ? 	jsonParser.readValueAs(Object[].class)
    						 : deserializeArrayAsTuple(jsonParser, sig);
    }	
	
	static Object[] deserializeArrayAsTuple (JsonParser jParser, Class<?>[] sig) throws IOException {// nextToken is the START_ARRAY
		int i = 0;
		if (jParser.currentToken() != JsonToken.START_ARRAY)
			throw new IOException("non tuple encountered where a tuple was expected");
		
		var tkn = jParser.nextToken(); //move past start_array //tkn is never read by the code, but is useful in the debugger
		var rslt = new Object[sig.length];
        for (var classz : sig) //TODO: the stream holds [t0...tn].  Need to do ONE deser with a type that reflects that!
    	   rslt[i++] = deserializeResult(jParser, classz, null);
        if ((tkn = jParser.nextToken())!= JsonToken.END_ARRAY)
		  throw new IOException("too many elements in a tuple");
        return rslt;
	}
	*/
}

