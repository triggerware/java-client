package nmg.softwareworks.jrpcagent;

import java.io.*;


/**
 * <p>A ServerConnection respresents a JRPCServer's connection to one specific agent that connected to it.
 * If a server wishes to keep track of some state specific to a particular connection,
 * it should subclass ServerConnection and override the server's  connect method
 * to return an instance of the subclass.
 * </p><p>
 * Each ServerConnection
 * </p>
 *
 */
public class ServerConnection extends Connection {
	private void initialize(ServerAgent sa) {
		this.setName(String.format("%s connection %d", sa.getServer().getName(), sa.getIndex()));}
	
	/*protected ServerConnection(ServerAgent sa,  InputStream istream, OutputStream ostream) throws IOException {
		super(sa, ostream, istream);
		initialize(sa);		
	}*/
	protected ServerConnection(ServerAgent sa, InputStream istream, OutputStream ostream) throws IOException {
		super(sa, istream, ostream);
		initialize(sa);
	}
}
