package calqlogic.twservercomms;

import nmg.softwareworks.jrpcagent.Connection;

/**
*In this version of the library, only two classes, QueryStatement and PreparedQuery,  implement statement,
* and the interface has only three methods.  Additional methods and implementions are anticipated in future versions.
*/
public interface Statement extends AutoCloseable{
	/**
	 * @return the Connection on which this Statement was created.
	 */
    Connection getConnection();
	
	//public default void releaseDependentResource(Object resource) {}

	/**
	 *closing a statement makes it no longer usable on its connection.
	 */
    void close();
}
