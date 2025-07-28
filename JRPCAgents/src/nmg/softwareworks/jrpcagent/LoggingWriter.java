package nmg.softwareworks.jrpcagent;

import java.io.*;
import java.nio.charset.StandardCharsets;

class LoggingWriter extends OutputStreamWriter{
	//private final Writer primary;
	private StringBuilder log = new StringBuilder(256);
	private boolean logging = false;
	LoggingWriter (OutputStream ostream){
		super(ostream, StandardCharsets.UTF_8);	}
	
	String getLoggedText(boolean reset) {
		var text = log.toString();
		if (reset) 
			if (log.length()>1028)
				log = new StringBuilder(256);
			else log.setLength(0);
		return text;
	}
	
	void setLogging(boolean b) {
		if (b == logging) return;
		log.setLength(0);
		logging = b;
	}

	@Override
	public void write(char[] cbuf, int offset, int len) throws IOException {
		if(logging) log.append(cbuf, offset, len);
		super.write(cbuf,offset,len);			
	}
	
}
