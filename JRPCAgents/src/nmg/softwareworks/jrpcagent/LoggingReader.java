package nmg.softwareworks.jrpcagent;

import java.io.IOException;
import java.io.InputStream;

class LoggingReader extends JRPCReader{
	/* This is used as the reader from which messages arriving from the server are parsed.
	 * As characters are pulled in from the stream by the parser, they are copied to a StringBuilder (the field named log)
	 * However, when messages arrive in rapid succession, the parser may pull in characters from message N+1 before it finishes
	 * parsing message N. So what we find in the "log" stringbuilder when we report it NEED NOT actually contain a single message.		 
	 */
	private StringBuilder log = new StringBuilder(256);
	private boolean logging = false;
	LoggingReader (InputStream is) throws IOException{
		super(is);}
	
	protected String getLoggedText(boolean reset) {
		var text = log.toString();
		if (reset) 
			if (log.length()>1028)
				log = new StringBuilder(256);
			else log.setLength(0);
		return text;
	}

	protected void setLogging(boolean b) {
		if (b == logging) return;
		log.setLength(0);
		logging = b;
	}
	
	@Override
	public int read() throws IOException{
		int character = super.read();
		if(logging && character!=-1) log.append((char)character);
		return character;
	}

	@Override
	public int read(char[] cbuf, int offset, int length) throws IOException{
		int howMany = super.read(cbuf,offset,length);
		if(logging && howMany>0) log.append(cbuf, offset, howMany);
		return howMany;
	}
}

