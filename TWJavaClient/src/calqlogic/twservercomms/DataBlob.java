package calqlogic.twservercomms;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import nmg.softwareworks.jrpcagent.JRPCException;


/**
 * A DataBlob holds binary data together with a mime type indicating the encoding of that data.
 * @author nmg
 *
 */
@JsonSerialize(using = DataBlob.BlobSerializer.class)
//@JsonDeserialize(using = BlobWithData.BlobDeserializer.class)
public class DataBlob{
	   private class DeferredBlobAccess{
			private final BlobReferenceSupport<?> brs;
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
				/*if (brs == null) {
					var proxy = ConnectorProxy.connectorFromName(connectorId);
					if (proxy != null && proxy instanceof BlobReferenceSupport)
						brs = (BlobReferenceSupport<?>)proxy;
				}*/
				return brs;
			}
	   }
	   private byte[]data;
	   private final String mimeType;
	   private final DeferredBlobAccess access;
	   /*public DataBlob(Blob blob, String mimeType) throws IOException, SQLException {
		   data = blob.getBinaryStream().readAllBytes();
		   this.mimeType = mimeType;
		   access = null;
	   }*/

	 /**
	 * creates a DataBlob from the bytes of an input stream
	 * @param stream  the stream containing the bytes of the data blob
	 * @param mimeType the mime type of the data
	 * @throws IOException if the stream cannot be read
	 */
	public DataBlob(InputStream stream, String mimeType) throws IOException {
	   data = stream.readAllBytes();
	   this.mimeType = mimeType;
	   access = null;
   }

	 /**
	 * creates a DataBlob from an array of bytes 
	 * @param data  the data bytes of the blob
	 * @param mimeType the mime type of the data
	 */
   public DataBlob(byte[] data, String mimeType) {
	   this.data = data;
	   this.mimeType = mimeType;
	   access = null;
   }
	   
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
   * @return the blob's binary data
   */
   public byte[] getData() {
		if (data == null) {
			try {
				var brs = access.getBrs();
				if (brs == null) return null;
				data = brs.downloadBlobContent(access.connectorKey);
			} catch (JRPCException e) {
				// TODO log something
				e=e;
			}}
		return data;
	}

    private static void serializeData(JsonGenerator gen, byte[]data) throws IOException {	
	   //writes the property "data" with the value being theb64 encoding of the data
	   gen.writeFieldName("data");
	   if (gen.canWriteBinaryNatively()) {
			gen.writeBinary(data);// uses Base64Variants.MIME_NO_LINEFEEDS
		} else {
			Base64.Encoder encoder = Base64.getEncoder();
			//untested means for writing b64 encoded data w/o first creating it as a string
			//gen.writeRaw('"');
			//OutputStream wrapped = encoder.wrap((OutputStream)gen.getOutputTarget()); // cannot CLOSE wrapped
			//wrapped.write(blob.data);
			//wrapped.flush();
			//gen.writeRawValue('"');
			var b64 = encoder.encodeToString(data);
			gen.writeString(b64);
		}
    }
	   
    static class BlobSerializer extends JsonSerializer<DataBlob> {
		@Override
		public void serialize(DataBlob blob, JsonGenerator gen, SerializerProvider arg2)
				throws IOException {
			gen.writeStartObject();
			gen.writeStringField("mimeType", blob.mimeType);
			if (blob.data != null) 
				serializeData(gen,blob.data);
			else {//TODO: implement for TW
				   /*gen.writeStringField("connector", 
						   blob.access.brs.getConnectorId());
				   gen.writeFieldName(Connector.connectorKeyPropertyName);
				   gen.writeTree(blob.access.connectorKey);*/
			}
			gen.writeEndObject();
		}
	 }
	   
	/*private static class DataBlobDeserializer extends JsonDeserializer<DataBlob>{
		    //public DataBlobDeserializer() {};
		    private DataBlob deserializeWithData(JsonNode json) {
			    var mimeType = json.get("mimeType").asText();
				var b64 = json.get("data").asText();
				Base64.Decoder decoder = Base64.getDecoder();
				return new DataBlob(decoder.decode(b64),mimeType);		   
		    } 
		    
		    private DataBlob deserializeDeferred(JsonNode json) {
			    //var mimeType = json.get("mimeType").asText();
			    return new DataBlob(json.get("connector").asText(), json.get("connectorKey"),
			    				    json.get("mimeType").asText());
		    }

			@Override
			public DataBlob deserialize(JsonParser parser, DeserializationContext arg1)
					throws IOException, JsonProcessingException {
				
				var json = parser.readValueAsTree();
				if (!json.isObject()) {
					//TODO: log something
					return null;
				}
				var oj = (ObjectNode)json;
				return  (oj.has("data")) ?  deserializeWithData(oj)
										 :  deserializeDeferred(oj);
			}
	   } //class DataBlobDeserializer
	   */
}

