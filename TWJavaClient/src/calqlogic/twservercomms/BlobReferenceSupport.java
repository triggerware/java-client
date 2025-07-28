package calqlogic.twservercomms;

import com.fasterxml.jackson.databind.JsonNode;

import nmg.softwareworks.jrpcagent.JRPCException;


interface BlobReferenceSupport <E extends JRPCException> {
	byte[] downloadBlobContent(JsonNode connectorKey) throws  E;
    String getConnectorId();
}

