package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.net.Socket;
import nmg.softwareworks.jrpcagent.JsonUtilities.JRPCObjectMapper;

/**
 * A ServerAgent is a JRPCAgent created in response to a JRPC Server receiving a connection request on it listening port.
 * It differs from an ordinary agent as follows:
 * <ul>
 * <li>If a ServerAgent has no registered request handler for a method, it will use a handler registered for the JRPCServer that 
 * spawned it.</li>
 * <li>If a ServerAgent has no registered notification inducer for a method, it will use a notification inducer
 *  registered for the JRPCServer that spawned it.</li>
 * </ul>
 */
public class ServerAgent extends JRPCAgent{
	
	private final JRPCServer server;
	private final int index;
	public ServerAgent(JRPCServer server, Socket socket) throws IOException{
		super(socket);
		this.server = server;
		this.index = server.nextConnectionIndex();
		try{server.onNewNetworkClient(this);
		}catch(Throwable t) {
			Logging.log(t, "onNewNetworkClient failed");
		}
	}
	
	/**
	 * @return the JRPCServer that created this agent
	 */
	public JRPCServer getServer() {return server;}
	final int getIndex() {return index;}
	/** Override this method to use a subclass of ServerConnection
	 * @param istream the stream on which requests (and possibly notifications) will arrive
	 * @param ostream the stream on which responses (and possibly notifications) will be sent
	 * @return the new ServerConnection
	 * @throws IOException
	 */
	/*private final ServerConnection connect(InputStream istream, OutputStream ostream) throws IOException {
		return new ServerConnection(this, istream, ostream);}*/
	
	void close() {server.removeClient(this);}
	
	@Override
	protected JRPCObjectMapper objectMapperForConnection(Connection c) {		
		return super.objectMapperForConnection(c);}
	
	//If this agent does not have its own registration for a handler, see if the Server itself has one


	@Override
	public NotificationInducer getNotificationInducer(String method) {
		var ni = super.getNotificationInducer(method);
		return (ni == null)? server.getNotificationInducer(method) : ni;
	}

	@Override
	public  RegisteredHandler getRequestHandler(String method) {
		var rh = super.getRequestHandler(method);
		return (rh == null) ? server.getRequestHandler(method) : rh;
	}
	
	@Override
	public  RequestSignature getRequestSignature(String method) {
		var rs = super.getRequestSignature(method);
		return (rs == null) ? server.getRequestSignature(method) : rs;
	}

}
