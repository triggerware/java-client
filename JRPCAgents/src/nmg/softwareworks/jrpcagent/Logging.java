package nmg.softwareworks.jrpcagent;

/**
 * <p>Logging provides static methods that let an application using this library
 * control the location and content of output produced by the logging statements within the library.
 * </p><p> A future version of this library will remove this class 
 * and use  one of the standard java configurable loggers for its logging.
 * </p>
 * @author nmg
 *
 */
public class Logging {
	/**
	 * An application must supply and implementation of the ILogger interface in order to cause
	 * this library to produce any logging output.
	 * @author nmg
	 *
	 */
	public interface ILogger{
		/**
		 * @param event a message to be logged
		 */
		default void log (String event) {}
		/**
		 * @param event a string indicating a problem that was encountered
		 * @param t a Throwable that was caught, leading to this logging request.
		 */
		default void log (Throwable t, String event) {}
		/**
		 * a format string an arguments to compute a message to be logged
		 * @param format a format string for java's String.format method
		 * @param args that java values to be consumed by the format string
		 */
		default void log (String format, Object ... args) {}		
	}
	/*private static String locateFile(String shortName) {
		try{
		    java.nio.file.Path source = java.nio.file.Paths.get(Logging.class.getProtectionDomain().getCodeSource().getLocation().toURI());
		    source = source.getParent();
		    source = source.resolve(shortName);
		    return source.toString();
		} catch(Throwable t){return null;}
	}*/

	//private static Logger defaultLogger = LogManager.getRootLogger();
    /*static { // this is all I could find to build a silent logger
    	var builder = ConfigurationBuilderFactory.newConfigurationBuilder();
    	builder.setStatusLevel(Level.ERROR);
    	builder.setConfigurationName("MuteConfig");
    	builder.add(builder.newRootLogger(Level.OFF));
    	Configurator.reconfigure(configuration);
    	var ctx = Configurator.initialize(builder.build()); 
    	defaultLogger = ctx.getRootLogger();
    }*/
    //private static Logger logger = defaultLogger;
    /*public static Logger configureLogging(File f) {
    	String oldPropVal = System.setProperty("log4j.configurationFile",f.getAbsolutePath());
    	logger = LogManager.getRootLogger();
    	if (oldPropVal != null)
    	  System.setProperty("log4j.configurationFile", oldPropVal);
    	else System.clearProperty("log4j.configurationFile");
    	return logger;
    }*/
	private static final ILogger emptyLogger = new ILogger() {};
	private static ILogger logger = emptyLogger;
	static void log(String event){ logger.log(event);}
	public static void log(Throwable t, String event){logger.log(t, event);}
	public static void log(String format, Object... args) {logger.log(format, args);}
	
	/**
	 * @return an ILogger instance that produces no logging output
	 */
	public static ILogger getEmptyLogger() {return emptyLogger;}

	/**
	 * @return the ILogger implementation currently being used by this library. Initially this is an empty logger.
	 */
	public static ILogger getLogger() {return logger;}
	
	/**
	 * @param logger the ILogger instance to use for future logging from this library. 
	 * @return the ILogger instance previously being used by this library
	 */
	public static ILogger setLogger(ILogger logger) {
		var previous = Logging.logger;
		Logging.logger = logger == null? emptyLogger : logger;
		return previous;
	}

	/*private static Object parseTest(String json, Class<?>classz) throws JsonParseException, IOException {
		MappingJsonFactory jfactory = new MappingJsonFactory();
		JsonParser jParser = jfactory.createParser(json);
		return jParser.readValueAs(classz);
		
	}*/
	static void main(String[] args) throws Exception {
		//String f = locateFile(args[0]);
		//configureLogging (new File(f));
		log("a simple message");
		log(null,new Exception("this is bad"));
    }
}

