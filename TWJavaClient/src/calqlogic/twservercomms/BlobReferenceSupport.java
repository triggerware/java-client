package calqlogic.twservercomms;

import com.fasterxml.jackson.databind.JsonNode;

import nmg.softwareworks.jrpcagent.JRPCException;


interface BlobReferenceSupport <E extends JRPCException> {
	public byte[] downloadBlobContent(JsonNode connectorKey) throws  E;
    public String getConnectorId();
}

