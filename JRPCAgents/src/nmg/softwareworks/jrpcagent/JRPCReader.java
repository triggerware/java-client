package nmg.softwareworks.jrpcagent;

import java.io.*;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

class JRPCReader  extends InputStreamReader{
	protected Object deserializationTarget = null;
	protected JRPCReader (InputStream is) throws IOException{
		super(is, StandardCharsets.UTF_8);}
	public Object getDeserializationTarget() {return deserializationTarget;}
	Object deserializeIntoObject(Object instanceForResult, JsonParser jParser) throws IOException {
		Object oldTarget = deserializationTarget;
		try {
			deserializationTarget = instanceForResult;
			var mapper = new ObjectMapper();
			JsonTimeUtilities.isoSerialization(mapper);
			return mapper.readerForUpdating(instanceForResult).readValue(jParser);
		}finally {deserializationTarget = oldTarget;}
	}
}
