package nmg.softwareworks.jrpcagent;

import java.util.HashMap;

/**
 * Every connection used by an Agent has an instance of this class available to both serialization and deserialization.
 * The entries in the map hold contextual state that is obtained during the serialization/deserialization of some JRPC message
 * and may be consumed later in the serialization/deserialization of that message.
 * 
 * All entries in a SerializationState are cleared before beginning the serialization/deserialization of a new JRPC message.
 *
 */
public class SerializationState extends HashMap<String, Object>{
	private final Connection connection;
	/**
	 * @param conn the connection that will use this SerializationState for serialization or deserialization
	 */
	public SerializationState(Connection conn) {
		super();
		this.connection = conn;
	}
	/**
	 * @return the connection using this SerializationState
	 */
	public Connection getConnection() {return connection;}
}
