package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.util.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.exc.InvalidNullException;
import com.fasterxml.jackson.databind.node.ObjectNode;



/**
 * Represents a single message received by an agent. either
 * a)a notification,
 * b)a request from a client, or
 * c)a response from a server
 *
 */
@JsonDeserialize(using = IncomingMessage.IncomingMessageDeserializer.class)
public class IncomingMessage implements IncomingJRPCMessage{
	private final Connection conn;
	HashMap<String,TreeNode> unexpected = null;//holds unexpected properties of json object
	TreeNode top = null;  //holds a top level message that is NOT a json object. This should never occur.
	String jsonrpc = null; // used for all messages. Just the value of the jsonrpc attribute of the message.
	String methodName = null; //used for request or notification
	Object[] positionalParams = null; //used for request or notification
	TreeNode unregisteredParams = null; //used for request or notification with no registration
	TreeNode invalidParams = null; // used when a params value is neither a json array nor a json object
	                                      // or when the form of params supplied is not the form registered
	Object id; //should allow int or string
	JRPCSimpleRequest<?> request = null; // the request to which this message is a response
	RequestSignature requestSignature = null;//the signature of the request being made by this message
	private Notification notification = null; //the notification deserialized from the params of this message
	Object result = null; // result is used for success responses
	boolean hasResult = false;  // distinguishes null result from no result!
	ObjectNode responseError = null;// an error sent as a response to a request
	JRPCRuntimeException resultDeserialingError = null; // while receiving a response
	JRPCRuntimeException paramsDeserializingError = null; //while receiving a request or notification
	Map<String, TreeNode> metaProperties = null;

	IncomingMessage(Connection conn){
		this.conn = conn;}
	
	public Object[] getPositionalParams() {return positionalParams;}
	void setResult(Object result) {
		this.result = result;
		this.hasResult = true;
	}
	
	@Override
	public Connection getConnection() {return conn;}
	
	public JRPCSimpleRequest<?> getRequest(){return request;}

	private String valueAsText(Object x) {
		return (x==null)?"null":x.toString();}
	@Override
	public String toString() {
		var sb = new StringBuffer();
		sb.append("jsonrpc=").append(valueAsText(jsonrpc)).append(',')
		  .append("id=").append(valueAsText(id)).append(',')
		  .append("method=").append(valueAsText(methodName)).append(',');
		  if (positionalParams != null) sb.append("params=").append(positionalParams.toString()).append(',');
		  //else if (jsonObjectParams!=null)sb.append("params=").append(jsonObjectParams.toString()).append(',');
		  sb.append("result=").append(valueAsText(result)).append(',')
		  .append("error=").append(valueAsText(responseError));
		return sb.toString();		
	}
	
	void ensureUnexpected() {
		if (unexpected == null) 
			unexpected =  new HashMap<String,TreeNode>();
	}

	void addMetaProperty(String propertyName, TreeNode value) {
		if (metaProperties ==  null) metaProperties = new HashMap<String,TreeNode>();
		metaProperties.put(propertyName, value);
	}

	public Map<String, TreeNode> getMetaProperties(){return metaProperties;}
	boolean isRequestOrNotification() {
		return methodName!=null;}	
	boolean isResponse() {
		return id != null && (hasResult || responseError != null || resultDeserialingError != null);}
	public boolean isJrpcMessage() {return "2.0".equals(jsonrpc);}
	//private String isJrpcRequest() {
	//	return (id!=null && params!=null) ? method : null;}
	
	private Object deserializeOneParameter(JsonParser jParser,  Object target) throws IOException {
		if (target instanceof Class<?> c)
			return conn.deserializeParameter(jParser, c);
		if (target instanceof TypeReference<?> tr)
			return jParser.readValueAs(tr);
		if (target instanceof JavaType jt)
			try {return conn.getPartnerMapper().readValue(jParser, jt);
			}catch(InvalidNullException e) {return null;}
		//should never occur
		Logging.log("invalid type in parameter signature %s", target);
		return conn.deserializeParameter(jParser, Object.class);		
	}

	private class ParamsFieldHandler extends JsonUtilities.FieldHandler{
		private final JsonParser jParser;
		private final JsonObjectRequestSignature requestSignature;
		private final Map<String, Object>jors;
		private final Object[] actualParameters;
		//content of jsonObjectParams will be added by calls to processFieldValue
		private ParamsFieldHandler(JsonParser jParser, JsonObjectRequestSignature requestSignature) {
			this.jParser = jParser;
			//this.forRequest = forRequest;
			jors = requestSignature.getParameterType();
			this.requestSignature = requestSignature;
			actualParameters = new Object[1+jors.size()];
			System.arraycopy(requestSignature.defaultActualParameters, 0,  actualParameters, 1, actualParameters.length-1);
			
		}
		//private NamedRequestParameters getNamedRequestParameters() {return jsonObjectParams;}
		@Override
		public void badJsonSyntax() {
			Logging.log("unexpected end of params on input stream");
			throw new JRPCRuntimeException.DeserializationFailure("");			
		}

		@Override
		public void processFieldValue(String attributeName) throws IOException {
			var requiredType = jors.get(attributeName);
			if (requiredType == null) {
				if (!requestSignature.isIgnored(attributeName)) 
					Logging.log("unexpected attribute %s in a params field. Deserializing as Object", attributeName);
				requiredType = Object.class;
			}
			var actualParam = IncomingMessage.this.deserializeOneParameter(jParser,  requiredType);
			int i = requestSignature.parameterIndex(attributeName);
			if (i>=0) {
				actualParameters[i+1] = actualParam;
			//var prev = jsonObjectParams.put(attributeName, actualParam);
			//if (prev != null)
			//	Logging.log("multiple instances of %s attribute in a params field", attributeName);
			} else //if (forRequest) 
				Logging.log("parameter named <%s> ignored", attributeName);			
		}
		
		/*public void fillMissingWithDefault() {
			//TODO: have processFieldValue keep track of which attributes have been processed
			// complain if any are duplicated
			// in this method, fill actual param of any missing attributes with the default for their type (currently it contains 
			// NULL, which is not correct for primitive types			
		}*/
		
		public Object[] getActualParameters() {return actualParameters;}
	}

	private void deserializeNotificationParams(JsonParser jParser, ObjectMapper mapper) {
		try {
			var inducer = conn.getAgent().getNotificationInducer(methodName);
			var notificationType = inducer == null? null : inducer.getNotificationType();
			if (notificationType == null) { // unregistered
				unregisteredParams = jParser.readValueAsTree();
				throw new JRPCRuntimeException.UnknownMethodFailure(methodName);
			}
			if (notificationType instanceof Class<?> c)
				notification = (Notification)jParser.readValueAs(c);
			//else if (notificationType instanceof TypeReference<?> tr)
			//	notification = (Notification)jParser.readValueAs(tr);
			else {
				//establish context for parsing the rows
				inducer.establishDeserializationAttributes(conn.getDeserializationState());
				notification = (Notification)mapper.readValue(jParser, (JavaType)notificationType);
			}
			notification.setInducer(inducer);
		}catch(IOException e) {
			throw new JRPCRuntimeException.DeserializationFailure("could not deserialize a notification",e);}
		if (notification ==  null)
			throw new JRPCRuntimeException.DeserializationFailure("could not deserialize a notification",null);
	}

	void deserializeRequestParams(JsonParser jParser) {
		try {
			var agent = conn.getAgent();
			requestSignature =  agent.getRequestSignature(methodName);
			if (requestSignature == null) { // unregistered
				unregisteredParams = jParser.readValueAsTree();
				throw new JRPCRuntimeException.UnknownMethodFailure(methodName);
			}
			if (requestSignature instanceof PositionalRequestSignature prs) {
				final var types = prs.getParameterTypes();
				positionalParams = new Object[types.length+1]; //0th element reserved for the ServerConnection instance
				if (jParser.currentToken() != JsonToken.START_ARRAY) {
					invalidParams = jParser.readValueAsTree();						
				} else {
					boolean tooFew = false, tooMany = false;
					int i = 1; //0th element reserved for the ServerConnection instance
					for (var type : types) {
						if (jParser.nextToken() == JsonToken.END_ARRAY) {
							tooFew = true;
							break; //leave for loop
						}
						positionalParams[i] = deserializeOneParameter(jParser, type);
						i++;
					}
					if (tooFew)
						throw new JRPCRuntimeException.DeserializationFailure(
								"too few actual parameters supplied in a request");
					while (jParser.nextToken() != JsonToken.END_ARRAY) {
						tooMany = true;
						jParser.readValueAsTree(); //parse and ignore extra param
					}
					if (tooMany)
						throw new JRPCRuntimeException.DeserializationFailure(
							"too many actual parameters supplied in a request");
					}
				} else if (requestSignature instanceof JsonObjectRequestSignature jors) {//should be true if it was not positional!
					if (jParser.currentToken() != JsonToken.START_OBJECT) {
						invalidParams = jParser.readValueAsTree();						
					} else {
						//var jors = (JsonObjectRequestSignature)requestSignature;
						var fieldHandler = new ParamsFieldHandler(jParser, jors);
						//jsonObjectParams = fieldHandler.getNamedRequestParameters();
						JsonUtilities.mapObjectFields(jParser, fieldHandler);
						//fieldHandler.fillMissingWithDefault();
						positionalParams = fieldHandler.getActualParameters();						
					}
				}				
			/*} else //this should not be possible
				unregisteredParams = jParser.readValueAsTree();*/
		}catch(IOException e) {
			throw new JRPCRuntimeException.DeserializationFailure("could not deserialize parameters of a request",e);
		}
	}
	
	@Override
	public void processMessage() throws Exception {			
		if (isRequestOrNotification()) {
			if (id != null) {
				conn.processRequestMessage(this);
		    } else {
		    	if (notification == null)
		    		Logging.log("unrecognized notification <%s>", this);
		    	else conn.enqueueNotification(notification, methodName);
		    }
		} else if (isResponse()) {
			conn.attachResponseToRequest(this);
		} else {
			var txt = String.format("processMessagesFromServer: Agent received <%s> from its partner.  It is not recognized as either a response or a notification. " +
					"Closing this connection.",  this);
			throw new Exception(txt);	
		}
	}

	@Override
	public boolean isNotification() {return notification!=null;	}

	@Override
	public boolean isBatch() {return false;}

	static class IncomingMessageDeserializer extends JsonDeserializer<IncomingMessage> {

		@Override
		public IncomingMessage deserialize(JsonParser jParser, DeserializationContext ctxt)	throws IOException, JacksonException {
			var dsstate= (SerializationState)ctxt.getAttribute("deserializationState");
			var conn = dsstate.getConnection();
			var message = new IncomingMessage(conn);
			JsonToken token;
			while (true) {//parse individual fields
				var propertyName = jParser.nextFieldName();
				if (propertyName==null) { 
					token = jParser.currentToken();
					if (token == JsonToken.END_OBJECT) return message;//break;
					Logging.log("unexpected end of message on input stream");
					return message;
				}
				switch(propertyName) {
					case "jsonrpc" ->{
						if (jParser.nextToken() == JsonToken.VALUE_STRING && message.jsonrpc == null)
							message.jsonrpc = jParser.getText();
						else conn.handleUnexpected(message, propertyName);
					}

					case "id" ->{//could be a request or a response. Not a notification
						//per jrpc spec, ids should be integer or string, and at most one id per message
							var firstId = true;
							jParser.nextToken();
							if (message.id != null) {
								firstId = false;
								Logging.log("Multiple id fields in a single message. Only use the first one");
							    conn.handleUnexpected(message, propertyName);
							} else	if (jParser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
								message.id = jParser.getIntValue();
							} else if (jParser.currentToken() == JsonToken.VALUE_STRING) {
								message.id = jParser.getText();
							} else {
								firstId = false;
								conn.handleUnexpected(message, propertyName);
							}
							if (firstId && message.methodName == null) {//could be response or request
								JRPCSimpleRequest<?> req = conn.pendingRequest(message.id);
								if (req != null) { //a response
									message.request = req;
									req.establishResponseDeserializationAttributes(message, ctxt);
									//conn.getSerializationState().setStateForParsing(req);
								}
							}
						}
					case "result" ->{ // should only occur on a response
						jParser.nextToken();
						Object rslt;
						if (message.request == null) {//this is a response, but we have no outstanding request!!
							Logging.log("Response received with no known request");
							rslt = jParser.readValueAsTree();
						} else
							try {
								rslt = message.request.deserializeResult(message, jParser, conn);
								//Logging.log("have result = [%s,%s]", rslt, rslt!=null?rslt.getClass():"no result");
							} catch (JRPCRuntimeException rte) {									
								var consumed = conn.getLoggedText(false);
								Logging.log(String.format("while deserializing result from %n %s", consumed));
								message.resultDeserialingError = rte;
								//break;
								throw rte;
							}
						message.setResult(rslt);
						//Logging.log("have result [%s]", rslt);
					}
					case "error" ->{ // should only occur on a response
						if (jParser.nextToken() == JsonToken.START_OBJECT && message.result == null) {
							message.responseError = jParser.readValueAsTree();
							Logging.log("have error[%s]", message.responseError);
						}else conn.handleUnexpected(message,propertyName);
					}
					case "method" ->{ // could be a request or a notification
						if (jParser.nextToken() == JsonToken.VALUE_STRING)
							message.methodName =  jParser.getText();
						else conn.handleUnexpected(message, propertyName);
						//Logging.log("have method field");
					}
					case "params" ->{ // could be a request or a notification
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
										message.deserializeNotificationParams(jParser, conn.getPartnerMapper()); }
								} catch (JRPCRuntimeException rte) {									
									var consumed = conn.getLoggedText(false);
									Logging.log(String.format("while deserializing params from %n %s", consumed));
									message.paramsDeserializingError = rte;
									//break;
									throw rte;
								}
							}  else	message.invalidParams = jParser.readValueAsTree();
							
						}
						else conn.handleUnexpected(message, propertyName);
						//Logging.log("have params field");
					}
					default ->{
						token = jParser.nextToken();
						if (conn.getAgent().allowsInboundProperty(propertyName)) {
							message.addMetaProperty(propertyName, jParser.readValueAsTree());
						} else {
							conn.handleUnexpected(message, propertyName,true);
						}
					}
				}
			}
		}
		
	}
}
