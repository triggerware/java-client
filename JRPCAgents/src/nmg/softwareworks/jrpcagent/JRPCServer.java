package nmg.softwareworks.jrpcagent;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *<p>A JRPCServer communicates with  JRPCAgents (it partners) using JsonRPC.  There is a single ServerConnection
 *for each client. The JRPCServer may receive requests and notifications from a partner on a ServerConnection, and may issue responses
 *and notifications on that ServerConnection.
 *</p>
 *
 */
public abstract class JRPCServer extends HandlerRegistration {
	private  int serverConnectionCounter = 1;
	final int nextConnectionIndex() {return ++serverConnectionCounter;}
	private final ServerSocket serverSocket ;
	private final String name;
	private final Set<ServerAgent>myClients = Collections.synchronizedSet(new HashSet<>());
	final void removeClient(ServerAgent sa) {myClients.remove(sa);}
	protected JRPCServer(String name, ServerSocket serverSocket) {
		this.name = name;
		this.serverSocket = serverSocket;
	}
	
	/**
	 * @return the port on which this server listens
	 */
	public int getPort() {return serverSocket.getLocalPort();}
	
	public String getName() {return name;}
	
	/**
	 * An JRPC Server implementation should override this method in order to use a subclass of ServerAgent for the agents
	 * created by connections to the host/port where it listens.
	 * A subclass would provide a means to hold per-client state over the lifetimee of a ServerAgent;
	 * @param socket  the server side connected Socket
	 * @return a new ServerAgent instance
	 * @throws IOException 
	 */
	public ServerAgent createServerAgent(Socket socket) throws IOException {
		return new ServerAgent(this,socket);}
	
	/**
	 * Override onNewNetworkClient to execute your own event handler when a new connection is made to this server.
	 */
	public void onNewNetworkClient(ServerAgent agent) {}
	private final Lock acceptLock = new ReentrantLock();
	public void blockFurtherConnections(boolean b) {
		if (b) acceptLock.lock(); else acceptLock.unlock();
	}

    public Thread start(boolean asDeamon) throws IOException {
		Thread thread = new Thread() {
			public void run() {
					while (true) {
						Socket socket = null;
						synchronized (acceptLock) {
							try {
								socket = serverSocket.accept();
							}catch(SocketException ie) {
								return;
							}catch (IOException e) {
								Logging.getLogger().log(e, "ServerSocket accept failure");
								break;
							}
						}
						try {
							myClients.add(createServerAgent(socket));
						}catch (Throwable t) {
							Logging.log(t, String.format("JRPCServer <%s>failed to create a new ServerAgent", name));
						}
					}
			}	
		};
		if (asDeamon) 
			thread.setDaemon(true);
		thread.start();
		return thread;
	}
	
	/**
	 * causes the JRPCServer to quit accepting new connections
	 */
	protected void quitListening() {
		try{serverSocket.close();
		}catch(IOException e) {}
    }
	
	/**
	 * performs a shutdown of each of the existing clients of this JRPCServer
	 */
	protected void shutdownAllClients() {
		myClients.forEach((sa -> {sa.shutdown();}));}
}
