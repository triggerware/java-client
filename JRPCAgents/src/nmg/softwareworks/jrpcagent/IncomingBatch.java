package nmg.softwareworks.jrpcagent;


public class IncomingBatch extends BatchMessage implements IncomingJRPCMessage{

	protected final Connection conn;

	//private final ArrayList<IncomingMessage> messages = new ArrayList<IncomingMessage>();  
	//elements of messages may be a combination or requests and notifications

	IncomingBatch(Connection conn) {
		this.conn = conn;
	}

	@Override
	public Connection getConnection() {return conn;}

	@Override
	public void processMessage() throws Exception {
		Logging.log("incoming batch messages are not yet handled");}

	@Override
	public boolean isNotification() {
		return false;}

	@Override
	public boolean isBatch() {
		return true;}
  
}
