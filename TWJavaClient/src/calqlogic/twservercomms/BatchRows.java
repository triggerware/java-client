package calqlogic.twservercomms;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ArrayNode;

import calqlogic.twservercomms.AbstractQuery.SignatureElement;
import nmg.softwareworks.jrpcagent.Logging;

@JsonDeserialize(using = BatchRows.BatchRowsDeserializer.class)
class BatchRows<T>{
	ArrayList<T> rows = new ArrayList<T>();
	ArrayList<T>getRows(){return rows;}
	@SuppressWarnings("unchecked")
	private void parseRows(JsonParser jParser, SignatureElement[] sig, Constructor<T>rowBeanConstructor, boolean validate) throws IOException {
		//positioned at start of first row (or end of array of rows)
		@SuppressWarnings("unused")
		JsonToken tkn;
		if (sig == FOLSignature) {
			var tnode = jParser.readValueAsTree();
			if (tnode instanceof ArrayNode anode)
				deserializeFOLRows(anode);
			return;
		}
		while (((tkn = jParser.currentToken()) == JsonToken.START_ARRAY)) {
			jParser.nextToken(); //consume the start array for the array of rows
			jParser.nextToken();//consume the start array token for a row
			var nextRow = parseOneRow(jParser, sig,  validate);
			if (nextRow == null) return;
			if (rowBeanConstructor != null) {
				//Object bean;
				try {
					T bean = rowBeanConstructor.newInstance(nextRow);
					rows.add(bean);
				} catch (Exception e) {
					Logging.log(e, String.format("constructor for <%s> failed", Constructor.class));
					return;}
			}
			else {rows.add((T)nextRow);	}	
		}
		if (consumeExcess(jParser)) 
			Logging.log("something other than a json array in the array of rows");
	}
	private boolean consumeExcess(JsonParser jParser) throws IOException {
		//consume any excess array elements,and then consume the end array token
		var hasExcess = false;
		while (jParser.currentToken() != JsonToken.END_ARRAY) {//consume and discard remaining elements of json array
			jParser.readValueAsTree();
			hasExcess = true;
		}
		return hasExcess;
	}

	@SuppressWarnings("unchecked")
	private void deserializeFOLRows(ArrayNode jrows) throws IOException {
		for (var tnode : jrows) {
			var nextRow = deserializeOneFOLRow((ArrayNode) tnode);
			rows.add((T)nextRow);
		}
	}

	private Object[] deserializeOneFOLRow(ArrayNode jrow) throws IOException {
	  var row = new Object[jrow.size()];
	  var index = 0;
	  for (var jval : jrow)
		  row[index++] = jsonValueToJava(jval);
	  return row;
	}
	
	private Object jsonValueToJava (JsonNode jval) {
		if (jval.isValueNode()) {
			if (jval.isNumber()) return jval.numberValue();
			if (jval.isTextual()) return jval.asText();
			if (jval.isNull()) return null;
			if (jval.isBoolean()) return jval.asBoolean();
			return null; //should be impossible
		}
		return null;
	}

	private Object[] parseOneRow(JsonParser jParser,  SignatureElement[] sig,  boolean validate) throws IOException {
		/*if (jParser.currentToken() != JsonToken.START_ARRAY) {//should never be true
			jParser.readValueAsTree();
			return null;
		}*/
		var row = new Object[sig.length];
		int index = 0;
		for (var se : sig) {
			var type = se.twSqlType;
			//jParser.nextToken();
			row[index] = TWBuiltInTypes.parseOneValue(jParser, type, validate);
			if (row[index] == null) return null;
			index++;
		}
		if (consumeExcess(jParser))	{		
			Logging.log("Deserializing a row : too many values");
			return null;
		}
		@SuppressWarnings("unused")
		var tkn = jParser.nextToken();
		return row;
	}

	final static SignatureElement[] FOLSignature = new SignatureElement[0];//indicates an FOL resultset, heuristic deserialization of row values, no knowledge of table width
	
	static class BatchRowsDeserializer<T> extends JsonDeserializer<BatchRows<T>> {
		@Override
		public  BatchRows<T> deserialize(JsonParser jParser, DeserializationContext ctxt)
				throws IOException, JacksonException {
			@SuppressWarnings("unchecked")
			var dsstate= (HashMap<String,Object>)ctxt.getAttribute("deserializationState");
			var sig = (SignatureElement[])dsstate.get("rowSignature");
			if (sig == null && dsstate.containsKey("FOL"))
				sig = FOLSignature;
				
			@SuppressWarnings("unchecked")
			var rowBeanConstructor = (Constructor<T>)dsstate.get("rowBeanConstructor");
			if (sig == null ) {
				jParser.readValueAsTree();
				throw new IOException("no row signature available for deserializing  batch rows");
			} else if (jParser.currentToken() != JsonToken.START_ARRAY) {
					var whatIsIt = jParser.readValueAsTree();
					throw new IOException(String.format("batch rows not serialized as a json array <%s>", whatIsIt));
			} else {
				//jParser.nextToken();
				var rslt = new BatchRows<T>();
				rslt.parseRows(jParser, sig, rowBeanConstructor, false);
				@SuppressWarnings("unused")
				var tkn = jParser.currentToken(); //should be end_array
				return rslt;
			}
		}
	}
}
