package calqlogic.twservercomms;

/*import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import nmg.softwareworks.jrpcclient.JsonUtilities;
import nmg.softwareworks.jrpcclient.JsonUtilities.JRPCReader;
*/



class RowArrayListDeserializer
	{/*

	static JavaType genType = TypeFactory.defaultInstance().constructParametricType(ArrayList.class,  Object[].class);
    public RowArrayListDeserializer() { 
    	super(genType);   } 

    @Override
    public ArrayList<Object[]> deserialize(JsonParser jsonParser, DeserializationContext dsContext) throws IOException {
    	var tuples = new ArrayList<Object[]>();
    	var twr = (JRPCReader)jsonParser.getInputSource();
    	var deserTarget = twr.getDeserializationTarget();
    	Class<?>[] sig = (deserTarget instanceof TupleSignatureHolder) ? ((TupleSignatureHolder)deserTarget).getTupleSignature() : null;
    	JsonToken tkn = jsonParser.currentToken(); //tkn is never read by the code, but is useful in the debugger
    	if (tkn != JsonToken.START_ARRAY) {//beginning of array of tuples
    		throw new IOException("tuples serialized as something other than a json array");}
    	while ((tkn=jsonParser.nextToken()) == JsonToken.START_ARRAY) {// another tuple
    		var element = JsonUtilities.parseTuple(jsonParser,sig); //leave currentToken as the end_array for the tuple
    		tuples.add(element);
    	}
	    if ((tkn=jsonParser.currentToken()) != JsonToken.END_ARRAY)
	    	throw new IOException("non tuple in a list of tuples");
    	return tuples;
    }*/	
}
