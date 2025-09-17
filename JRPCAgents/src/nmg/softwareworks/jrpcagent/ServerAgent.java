package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.net.Socket;

//import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * A ServerAgent is a JRPCAgent created in response to a JRPC Server receiving a connection request on it listening port.
 * It differs from an ordinary agent as follows:
 * <ul>
 * <li>If a ServerAgent has no registered request handler for a method, it will use a handler registered for the JRPCServer that 
 * spawned it.</li>
 * <li>If a ServerAgent has no registered notification inducer for a method, it will use a notification inducer
 *  registered for the JRPCServer that spawned it.</li>
 * </ul>
 * A subclass of this class should be used as the 'typical' kind of agent one obtains by connecting to a specific JRPC service over a socket connection.
 * The subclass will register the request and notification handlers for the protocol.
 * 
 * This library does not have a corresponding clientAgent class.  It should.
 * 
 */
public abstract class ServerAgent extends JRPCAgent{
	
	private final JRPCServer server;
	private final int index;
	/**
	 * @param server The JRPCServer whose behavior this agent implements
	 * @param socket The connection that identifies the partener agent of this ServerAgent
	 * @param name A name for this agent. This can be null. The agent name may be assigned later with setName()
	 * @throws IOException if a problem arises establishing the communications channels between the ServerAgent and its partner
	 */
	public ServerAgent(JRPCServer server, Socket socket, String name) throws IOException{
		super(socket, name);
		this.server = server;
		this.index = server.nextConnectionIndex();
		server.onNewNetworkClient(this);
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
	
	@Override
	public void close() {server.removeClient(this);}
	
	/*@Override
	protected JsonMapper objectMapperForConnection(Connection c) {		
		return c.getObjectMapper();}*/
	
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
