package nmg.softwareworks.jrpcagent;

interface IncomingJRPCMessage {
	void processMessage() throws Exception;
	Connection getConnection();
	boolean isNotification();
	boolean isBatch();
}
