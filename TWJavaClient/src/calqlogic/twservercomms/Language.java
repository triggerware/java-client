package calqlogic.twservercomms;

/**
 * Language provides string constants that are used in TW server requests that
 * require the client to identify the language in which a query string is written.
 * @author nmg
 *
 */
public abstract class Language {
	/**
	 * The TW server's FOL language
	 */
	public final static String FOL = "fol";
	/**
	 * The TW server's SQL language
	 */
	public final static String SQL = "sql";
}
