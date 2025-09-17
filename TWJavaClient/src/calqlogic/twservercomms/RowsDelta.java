package calqlogic.twservercomms;

import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

//@JsonDeserialize(using = RowsDelta.RowsDeltaDeserializer.class)
public class RowsDelta<T> {

	//@JsonProperty("added")
	private /*final*/ ArrayList<T> added;
	//@JsonProperty("deleted")
	private /*final*/ ArrayList<T> deleted;
	private final ArrayList<T> noRows = new ArrayList<T>(0);
	
	//public RowsDelta() {} //for default jackson deserializer

	@JsonCreator
	private RowsDelta(@JsonProperty(value="added", required = false) BatchRows<T> added, @JsonProperty(value="deleted", required = false) BatchRows<T> deleted){
		this.added = added == null ? noRows : added.rows;
		this.deleted = deleted == null ? noRows : deleted.rows;
	}
	
	public ArrayList<T>getAdded(){return added;}
	public ArrayList<T>getDeleted(){return deleted;}
	
	
	/*static class RowsDeltaDeserializer<T> extends JsonDeserializer<RowsDelta<T>> {
		@Override
		public  RowsDelta<T> deserialize(JsonParser jParser, DeserializationContext ctxt)
				throws IOException, JacksonException {
			@SuppressWarnings("unchecked")
			var dsstate= (HashMap<String,Object>)ctxt.getAttribute("deserializationState");
			var sig = (SignatureElement[])dsstate.get("rowSignature");
			@SuppressWarnings("unchecked")
			var rowBeanConstructor = (Constructor<T>)dsstate.get("rowBeanConstructor");
			if (sig == null ) {
				jParser.readValueAsTree();
				throw new IOException("no row signature available for deserializing a polled query's delta rows");
			} else if (jParser.currentToken() != JsonToken.START_OBJECT) {
					var whatIsIt = jParser.readValueAsTree();
					throw new IOException(String.format("delta rows not serialized as a json object <%s>", whatIsIt));
			} else {
				jParser.nextToken(); // consume open object token
				ArrayList<T> added = null, deleted = null;
				while (true) {//parse individual fields
					var propertyName = jParser.nextFieldName();
					if (propertyName==null) { 
						var token = jParser.currentToken();
						if (token == JsonToken.END_OBJECT) {
							jParser.nextToken(); // consume end object
							return new RowsDelta<T>(added, deleted);
						} else {
							Logging.log("unexpected end of polled query notification on input stream");
							return new RowsDelta(null, null);
						}
					}
					switch(propertyName) {
						case "added" ->{
							var rows = jParser.readValueAsTree();
							rows = rows;
						}

						case "deleted" ->{//could be a request or a response. Not a notification
							var rows = jParser.readValueAsTree();
							rows = rows;
							}
						default ->{
							Logging.log("unknown json key <%s> in a polled query delta notification. Value ignored", propertyName);
							jParser.readValueAsTree();
						}
					}
				}
			}
		}
	}*/
}
