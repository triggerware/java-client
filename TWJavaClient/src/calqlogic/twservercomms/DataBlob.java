package calqlogic.twservercomms;

import java.io.IOException;
import java.util.Base64;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


/**
 * <p>A DataBlob holds binary data together with a mime type indicating the encoding of that data.
 * It also may hold the base64 encoding of that data.
 * It is intended, but cannot be enforced, that the binary data is immutable.
 * The implementation caches the base64 encoding, and that cached encoding would become invalid were
 * the data to change.
 * </p><p> * 
 * Although TW SQL has a column type for blobs, the current implementation does not make it be of much use.
 * The blob type it TW SQL has no associate MIME type, and provides no way for a client to share a <em>reference</em> to the data with the TW server.
 * In cases where datasources offer data that is best modeled as a binary value, if it is possible to obtain a URL from which the
 * data can be retrieved (e.g., via an http request) an application can just use the URL as the value in a column and the client can
 * obtain the data via the URL without any involvement of the TW server. 
 * </p>
 *
 */
@JsonSerialize(using = DataBlob.BlobSerializer.class)
@JsonDeserialize(using = DataBlob.BlobDeserializer.class)
public class DataBlob{
	   /*private class DeferredBlobAccess{
			private BlobReferenceSupport<?> brs;
			private final JsonNode connectorKey;
			private final String connectorId;
			private DeferredBlobAccess(String connectorId, JsonNode connectorKey) {
				this.brs = null;
				this.connectorId = connectorId;
				this.connectorKey = connectorKey;
			}
			private DeferredBlobAccess(BlobReferenceSupport<?> brs, JsonNode connectorKey) {
				this.brs = brs;
				this.connectorId = brs.getConnectorId();
				this.connectorKey = connectorKey;
			}
			BlobReferenceSupport<?> getBrs() {
				//TODO: implement for TW
				//if (brs == null) {
				//	var proxy = ConnectorProxy.connectorFromName(connectorId);
				//	if (proxy != null && proxy instanceof BlobReferenceSupport)
				//		brs = (BlobReferenceSupport<?>)proxy;
				//}
				return brs;
			}
	   }*/
	   private String encoding;
	   private byte[]data;
	   private final String mimeType;
	   //private final DeferredBlobAccess access;
	   /*public DataBlob(Blob blob, String mimeType) throws IOException, SQLException {
		   data = blob.getBinaryStream().readAllBytes();
		   this.mimeType = mimeType;
		   access = null;
	   }*/
	 /**
	  * create a new DataBlob with encoding and mime type provided
	 * @param encoding the base64 encoding of the data
	 * @param mimeType  a mime type for the data
	 */
	public DataBlob(String encoding, String mimeType) {
		 this.encoding = encoding;
		 this.data = Base64.getDecoder().decode(encoding);
		 this.mimeType = mimeType;
	 }

	 /**
	  * create a new DataBlob with encoding provided and using the mime type <em>application/octet-stream</em>
	 * @param encoding the base64 encoding of the data
	 */
	public DataBlob(String encoding) {
		 this(encoding, "application/octet-stream" );}

	 /**
	 * creates a DataBlob from the bytes of an input stream
	 * @param stream  the stream containing the bytes of the data blob
	 * @param mimeType the mime type of the data
	 * @throws IOException if the stream cannot be read
	 */
	/*public DataBlob(InputStream stream, String mimeType) throws IOException {
	   data = stream.readAllBytes();
	   this.mimeType = mimeType == null ? "application/octet-stream" : mimeType;
	   encoding = null;
   }*/

	 /**
	 * creates a DataBlob from an array of bytes 
	 * @param data  the data bytes of the blob
	 * @param mimeType the mime type of the data
	 */
   public DataBlob(byte[] data, String mimeType) {
	   this.data = data;
	   this.mimeType = mimeType;
	   encoding = null;
	   //access = null;
   }
   public DataBlob(byte[] data) {
	   this(data,"application/octet-stream");   }
	   
   /*public DataBlob(String connectorId, JsonNode connectorKey, String mimeType) {
	   data = null;
	   this.mimeType = mimeType;
	   access = new DeferredBlobAccess(connectorId,connectorKey);
   }
   
   public DataBlob(BlobReferenceSupport<?> brs, JsonNode connectorKey, String mimeType) {
	   data = null;
	   this.mimeType = mimeType;
	   access = new DeferredBlobAccess(brs,connectorKey);
   }*/
	   
   /**
   * @return the mime type for the blob's binary data
   */
   public String getMimeType() {return mimeType;}

   /**
   * @return the blob's binary data as a byte array
   */
   public byte[] getData() {
		/*if (data == null) { //deferred access
			try {
				var brs = access.getBrs();
				if (brs == null) return null;
				data = brs.downloadBlobContent(access.connectorKey);
			} catch (JRPCException e) {
				// TODO log something
				e=e;
			}}*/
		return data;
	}
    /**
     * @return the MIME type assigned to this data
     */
    public String getEncoding() {
    	if (encoding == null)
    		encoding = Base64.getEncoder().encodeToString(data);
    	return encoding;
    }
   

    /**
     * Encodes a blob as a Json string that holds the base64 encoding of that string.
     * @param gen the JsonGenerator
     * @param data the bytes to encode onto the generator's output stream
     * @throws IOException
     */
    private static void serializeData(JsonGenerator gen, byte[]data) throws IOException {	
	   //gen.writeFieldName("data");	 
		Base64.Encoder encoder = Base64.getEncoder();
		//untested means for writing b64 encoded data w/o first creating it as a string
		/*gen.writeRaw('"');
		var wrapped = encoder.wrap((java.io.OutputStream)gen.getOutputTarget()); // cannot CLOSE wrapped
		wrapped.write(data);
		wrapped.close();
		gen.writeRaw('"');*/
		var b64 = encoder.encodeToString(data);
		gen.writeString(b64);
    }
	   
   static class BlobSerializer extends JsonSerializer<DataBlob> {
		@Override
		public void serialize(DataBlob blob, JsonGenerator gen, SerializerProvider arg2)
				throws IOException {
			//gen.writeStartObject();
			//gen.writeStringField("mimeType", blob.mimeType);
			if (blob.data != null) 
				serializeData(gen,blob.data);
			else gen.writeNull();
			{//TODO: implement for TW
				   /*gen.writeStringField("connector", 
						   blob.access.brs.getConnectorId());
				   gen.writeFieldName(Connector.connectorKeyPropertyName);
				   gen.writeTree(blob.access.connectorKey);*/
			}
			//gen.writeEndObject();
		}
	 }
	   
	static class BlobDeserializer extends JsonDeserializer<DataBlob>{
		/*private DataBlob deserializeWithData(JsonNode json) {
			    var mimeType = json.get("mimeType").asText();
				var b64 = json.get("data").asText();
				Base64.Decoder decoder = Base64.getDecoder();
				return new DataBlob(decoder.decode(b64),mimeType);		   
		    } 
		    
		    private DataBlob deserializeDeferred(JsonNode json) {
			    //var mimeType = json.get("mimeType").asText();
			    return new DataBlob(json.get("connector").asText(), json.get("connectorKey"),
			    				    json.get("mimeType").asText());
		    }*/

			@Override
			public DataBlob deserialize(JsonParser parser, DeserializationContext arg1)
					throws IOException, JsonProcessingException {
				
				var encoding = parser.readValueAs(String.class);
				return  new DataBlob(encoding);
			}
	   } 
}

