package calqlogic.twservercomms.testing;


import java.math.BigInteger;
import java.net.InetAddress;
import java.time.*;
import java.util.HashMap;
import java.util.List;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.*;

import calqlogic.twservercomms.AbstractQuery.SignatureElement;
import calqlogic.twservercomms.*;
import calqlogic.twservercomms.PolledQuery.PolledQueryControlParameters;
import calqlogic.twservercomms.PreparedQuery.PositionalParameterDeclarations;
import calqlogic.twservercomms.TWResultSet.TWResultSetException;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.*;
import nmg.softwareworks.jrpcagent.Logging.ILogger;

class Test {
	
	static class TestClient extends TriggerwareClient {

		public TestClient(InetAddress twHost, int twServerPort) throws Exception {
			super("test client", twHost, twServerPort);	}
		
		private static PositionalParameterRequest<Void> noopRequest = 
				new PositionalParameterRequest<Void>(Void.TYPE, "noop", 0, 0);	
		void noop()throws JRPCException {
			noopRequest.execute(primaryConnection);	}		
	}

	/*private static void parserTest(Class<?>cls, String json) {
		 var instream =   new ByteArrayInputStream(json.getBytes()); 
		 var deserializationState = new SerializationState(null);
		 var jm = JsonUtilities.jsonMapper(deserializationState);
		 try {
			 var parsed = jm.readValue(instream, cls);
			 parsed = parsed;
		} catch (Exception e) {
			e = e;
		}
	}*/
	/*private static void parserTest(TypeReference jtr, String json) throws IOException {
		 var instream =   new ByteArrayInputStream(json.getBytes()); 
		 var dsstate = new SerializationState(null);
		 dsstate.put("xyz", 123);
		 var jm = JsonUtilities.jsonMapper(dsstate);
		 //var parser = jm.createParser(instream);
		 try {
			 var parsed = jm.readValue(instream,jtr);
			 parsed = parsed;
		} catch (Exception e) {
			e = e;
		}
	}

	/*private static void pqNotificationDeserializeTest () {
		 var now = Instant.now().toString();
		 //var pqNotification = "{\"handle\" : 3, \"timestamp\" : \"" +now + "\", \"error\" : \"this is wrong\"}";
		 var delta = "{\"added\" :[[1]], \"deleted\" : [[2]] }";
		 var pqNotification = "{\"handle\" : 3, \"timestamp\" : \"" +now + "\", \"delta\" :"  + delta + '}';
		 //{"handle" : 3, "timestamp" : "2021-11-18T18:37:44.247149400Z", "delta" :{"added" :[[1]], "deleted" : [[2]] }}
		 var instream = isFromString(pqNotification);
		 var om = new ObjectMapper( new MappingJsonFactory());
		 JsonTimeUtilities.isoSerialization(om);
		 var jt = om.getTypeFactory().constructParametricType(PolledQueryNotification.class, int[].class);
		 try {
			 PolledQueryNotification<int[]> pqn = om.readValue(instream,jt);
			 pqn = pqn;
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	}*/

	/*private static QueryResponse<PerformanceTesting.IntegerPair> queryResponseDeserializeTest() throws  IOException {
		var jfactory = new MappingJsonFactory();
		var om = new ObjectMapper(jfactory);
		var jParser = jfactory.createParser(
				"{\"columns\" : [\"col1\",\"col2\"], \"exitstatus\":\"finished\", \"result\": [[12,13],[15,17]]} ");

		var jt = om.getTypeFactory().constructParametricType(QueryResponse.class, PerformanceTesting.IntegerPair.class);
		var xx = om.readValue(jParser,jt)	;		
		return (QueryResponse<PerformanceTesting.IntegerPair>)xx;
	}*/
	/*private static PreparedQueryResult<PerformanceTesting.IntegerPair> testPreparedQuery() throws JsonParseException, IOException {var jfactory = new MappingJsonFactory();
	     var om = new ObjectMapper(jfactory);
		 var jParser = jfactory.createParser(
		"{\"handle\":4, " 
			+  " \"inputSignature\":[{\"attribute\":\"?col1Min\",\"type\":\"number\"},{\"attribute\":\"?col2Max\",\"type\":\"number\"}],"
			+  "    \"signature\":[{\"attribute\":\"COL1\",\"type\":\"double\"},{\"attribute\":\"COL2\",\"type\":\"double\"}],"
			+  "   \"usesNamedParameters\":true}");
		var jt = om.getTypeFactory().constructParametricType(PreparedQueryResult.class, PerformanceTesting.IntegerPair.class);
		var xx = om.readValue(jParser,jt)	;
		return (PreparedQueryResult<PerformanceTesting.IntegerPair>)xx;
	}*/
	/*static void testPreparedQuery(TestClient twc) throws JRPCException, TriggerwareClientException {
		var query = "select * from bizfilefind, bizfiledetails where searchfor= :company and bizfilefind.id = bizfiledetails.id";
		try (var pq = new PreparedQuery<Object[]>(Object[].class,query, Language.SQL,"AP5", null, twc)){
			pq.setParameter("company", "ibm");
			var rs = pq.executeQuery();
			rs = rs;
		}
	}*/
	/*private static TWResultSet<Object[]> testPreparedQuery(TestClient client) throws JRPCException, TriggerwareClientException{
		var pq = new PreparedQuery<Object[]>(Object[].class, 
				"select * from [csimarket-competition] where  companysymbol = 'AMGN'", 
				 "AP5", client);
		var rs = pq.executeQuery();
		return rs;
	}*/

	public static class JustTheTimestamp{
		final Instant time;
		@DeserializationConstructor
		public JustTheTimestamp(Instant time){
			  this.time = time;}
		@Override
		public String toString() {
			return String.format("JTS: %s", time);
		}
	}

	public static class JustTheDuration{
		final Duration dur;
		@DeserializationConstructor
		public JustTheDuration(Duration dur){
			  this.dur = dur;}
		@Override
		public String toString() {
			return String.format("JTS: %s", dur);
		}
	}

	private static class PolledTimestampQuery extends PolledQuery<JustTheTimestamp>{
		//for testing unscheduled polling
		private final int nPolls; // number of polling operations
		private final Object pollingDone; //to signal caller when nPolls have reported
        private int pollsSoFar = 0;
		PolledTimestampQuery(TriggerwareClient client, int nPolls, Object pollingDone) throws JRPCException, TriggerwareClientException{
			super(client, JustTheTimestamp.class,  "SELECT current_timestamp() as now",  "AP5-SQL-RUNTIME",
					new PolledQueryControlParameters(true, true, null));
			this.nPolls = nPolls;
			this.pollingDone = pollingDone;
		}
		private void noMorePolling() {
			this.close();
			synchronized(pollingDone) {pollingDone.notify();}			
		}

		@Override
		public void handleSuccess(RowsDelta<JustTheTimestamp> delta, Instant ts) {//the ts is the timestamp of the delta, nothing to do with the data items!!!
			Logging.log("SamplePolledQuery reports change as of %s.%n added=%s deleted=%s", 
					ts, delta.getAdded(), delta.getDeleted());
			pollsSoFar++;
			if (pollsSoFar < nPolls)
				try {
					Thread.sleep(5000);
					this.poll();
				} catch (Exception e) {}
			else noMorePolling();
		}
		@Override
		public void handleError(String errorText, Instant ts) {
			super.handleError(errorText, ts);
			pollsSoFar++;
			if (pollsSoFar < nPolls)
				try {this.poll();
				} catch (Exception e) {}
			else noMorePolling();
		}
	}
	private static class ScheduledTimestampQuery extends ScheduledQuery<JustTheTimestamp> {
		
		//static TypeReference<PolledQueryNotification<int[]>> tr = new TypeReference<PolledQueryNotification<int[]>>() {};
		ScheduledTimestampQuery(TriggerwareClient client, PolledQuerySchedule schedule) throws JRPCException, TriggerwareClientException{
			super(client, JustTheTimestamp.class,  "SELECT current_timestamp() as now",  "AP5-SQL-RUNTIME", schedule,
					new PolledQueryControlParameters(true, true, null));}

		@Override
		public void handleSuccess(RowsDelta<JustTheTimestamp> delta, Instant ts) {//the ts is the timestamp of the delta, nothing to do with the data items!!!
			Logging.log("SamplePolledQuery reports change as of %s.%n added=%s deleted=%s", 
					ts, delta.getAdded(), delta.getDeleted());}
		@Override
		public void handleError(String errorText, Instant ts) {
			super.handleError(errorText, ts);	}
	}
	private static class ScheduledDurationQuery extends ScheduledQuery<JustTheDuration> {
		
		//static TypeReference<PolledQueryNotification<int[]>> tr = new TypeReference<PolledQueryNotification<int[]>>() {};
		ScheduledDurationQuery(TriggerwareClient client, PolledQuerySchedule schedule) throws JRPCException, TriggerwareClientException{
			super(client, JustTheDuration.class,  "SELECT current_timestamp() - TIMESTAMP '2025-04-15 12:00:00'  as elapsed",  "AP5-SQL-RUNTIME", schedule,
					new PolledQueryControlParameters(true, true, null));}

		@Override
		public void handleSuccess(RowsDelta<JustTheDuration> delta, Instant ts) {//the ts is the timestamp of the delta, nothing to do with the data items!!!
			Logging.log("SamplePolledQuery reports change as of %s.%n added=%s deleted=%s", 
					ts, delta.getAdded(), delta.getDeleted());}
		@Override
		public void handleError(String errorText, Instant ts) {//just log
			super.handleError(errorText, ts);	}
	}
	/*public class JustOneNumber{
		Integer n;
		@DeserializationConstructor
		public JustOneNumber(Integer n) {
			this.n = n;}
	}*/
	public static class JustNumbers{
		final BigInteger bi;
		final Long l;
		final Integer n;
		final Short s;
		final Byte b;
		final Double d;
		final Float f;
		@DeserializationConstructor
		public JustNumbers(BigInteger bi, Long l, Integer n, Short s, Byte b, Double d, Float f) {
			this.bi = bi; this.l = l; this.n = n; this.s = s; this.b = b;
			this.d = d; this.f = f;
			}
	}
	public static class JustText{
		final String sensitive;
		final String insensitive;
		@DeserializationConstructor
		public JustText(String s, String is) {
			sensitive = s; insensitive = is;}
	}
	public static class PubmedRecord{
		final String  id;
		final String title;
		@DeserializationConstructor
		public PubmedRecord(String id, String title) {
			this.id = id; this.title = title;}
		
	}
	public static class InflationRecord{
		final int year1;
		final int year2;
		final Float factor;
		@DeserializationConstructor
		public InflationRecord(int year1, int year2, Float factor) {
			this.year1 = year1; this.year2 = year2; this.factor = factor;}
	}
	public static class FOLQueryStatement extends QueryStatement{
		public FOLQueryStatement(TriggerwareClient client) {
			super(client);
		}

		@Override
		protected NamedRequestParameters commonParams(String query, String schema) {
			return new NamedRequestParameters().with("query", query).with("language", "fol").with("namespace", schema);}

		@Override
	    public void establishResponseDeserializationAttributes(JRPCSimpleRequest<?> request, IncomingMessage response, DeserializationContext ctxt) {
			@SuppressWarnings("unchecked")
			var dsstate= (HashMap<String,Object>)ctxt.getAttribute("deserializationState");
			dsstate.put("FOL", true);	
	    }
	}

	private static void showRow(Object[] row) {
		var sb = new StringBuilder();
		for (var obj : row) 
			sb.append(obj.toString()).append("  ");
		Logging.log(sb.toString());
	}
	/*public static String testResult = 
		{"signature":[{"attribute":"_SEL__1","type":"anyint"},{"attribute":"_SEL__2","type":"bigint"},{"attribute":"_SEL__3","type":"integer"},{"attribute":"_SEL__4","type":"smallInt"},{"attribute":"_SEL__5","type":"tinyInt"},{"attribute":"_SEL__6","type":"Double"},{"attribute":"_SEL__7","type":"Float"}],
				"batch":{"count":1,"exhausted":true,"tuples":[[3000000,300000,30000,3000,35,2238.769987,2238.77]]}}*/
	private static void testAdhocQueries(TriggerwareClient client) 
			throws JRPCException, TriggerwareClientException {

		String sql; SignatureElement[]sig;

		try(var fqs = new FOLQueryStatement(client)){
			fqs.setFetchSize(6);
			var fol = "((factor) s.t. (demo::inflation 1995 2000 factor))";			

			 try (var rsxxx = fqs.executeQuery(Object[].class, fol, "AP5")){
				while (rsxxx.next()) {
					var nextRow = rsxxx.get();
					showRow((Object[])nextRow);
				}
			}catch(Exception rse) {
				rse = rse;			}
			fol = "((FACTOR ) s.t.  (E () (AND (INFLATION 1990  2000  FACTOR ))))";		
			try (var rsxxx = fqs.executeQuery(Object[].class, fol, "AP5")){
				while (rsxxx.next()) {
					var nextRow = rsxxx.get();
					showRow((Object[])nextRow);
				}
			}catch(Exception rse) {
				rse = rse;	}

			fol = "((TITLE ID TYPE FORMEDIN AGENT FILINGDATE ) s.t.(E () (AND (BIZFILE-FIND \"Google\"  TITLE  ID  TYPE  FORMEDIN  AGENT  FILINGDATE ))))";
			try (var rsxxx = fqs.executeQuery(Object[].class, fol, "AP5")){
				while (rsxxx.next()) {
					var nextRow = rsxxx.get();
					showRow((Object[])nextRow);
				}
			}catch(Exception rse) {
				rse = rse;	}
		}
		
		try(var qs = new QueryStatement(client)){
			sql = "SELECT 55";
			var rsxxx = qs.executeQuery(Object[].class, sql, "AP5");
			if (rsxxx.next()) {
				var nextRow = (Object[])rsxxx.get();
				Logging.log("returned [%d]", nextRow[0]);
			}
			
			sql = "SELECT * FROM inflation WHERE year1 = 1998 AND year2 = 2006";
			var rsPMS = qs.executeQuery(InflationRecord.class, sql, "DEMO");
			sig = rsPMS.getRowSignature();
			try {
				while (rsPMS.next()) {
					var nextRow = (InflationRecord)(rsPMS.get());
					Logging.log("year1 = %d, year2 = %d, factor = %f", nextRow.year1, nextRow.year2, nextRow.factor);
				}
			}catch(Exception rse) {
				rse = rse; }

			sql = """
					SELECT CAST(3000000 as ANYINT), CAST(300000 AS BIGINT), CAST (30000 AS INTEGER),  CAST (3000 AS SMALLINT), CAST(35 AS TINYINT),
					CAST(2238.769987 AS DOUBLE), CAST(2238.769987 AS FLOAT)
					""";
		   var rsNum = qs.executeQuery(JustNumbers.class, sql, "DEMO");
		   sig = rsNum.getRowSignature();
		   sql = "SELECT 'abc', 'xyz' COLLATE ci ";
		   var rsText = qs.executeQuery(JustText.class, sql, "DEMO");
		   sig = rsText.getRowSignature();
		   //sig = sig;
		   
		   sql = "SELECT Current_timestamp() as now, current_date()as today, current_time() as clock, INTERVAL 3 DAY as howlong";
		   var rsTemporal = qs.executeQuery(TemporalFields.class, sql, "DEMO");
		   sig = rsTemporal.getRowSignature();
			/*sql = 
					"SELECT * FROM device WHERE MANUFACTURER_D_NAME LIKE '%MEDTRONIC%' ";
			rs = qs.executeQuery(sql, "AP5");	
			rs=rs;*/
		}
		try(var qs = new QueryStatement(client)) {
			var noMoreRows = new Object();
			var controller = new NotificationResultController<PubmedRecord>(PubmedRecord.class,  2 , null, null) {//6 rows, 2 at a time
				@Override
				public boolean handleRows(List<PubmedRecord> rows, boolean exhausted) {
					for (var pm : rows) {
						Logging.log("notification of PubmedRecord: id = %s, title = %s", pm.id, pm.title);	}
					if (exhausted) {
						Logging.log("no more notifications of PubmedRecord");	
						synchronized(noMoreRows) {noMoreRows.notify();}
						return false;
					}
					return true;
				}};
			sql = "SELECT pmid,title FROM pubmed_search WHERE query='toenail' AND title LIKE '%o%'";
			qs.executeQueryWithNotificationResults(PubmedRecord.class, sql,  "DEMO",  controller);
			synchronized(noMoreRows) {try {noMoreRows.wait();
			} catch (InterruptedException e) {}}
		}
	}
	private static void testPreparedQueries(TriggerwareClient client) 
			throws JRPCException, TriggerwareClientException {

		var pdecls = new PositionalParameterDeclarations(); pdecls.add("integer");
		try(var pq = new PreparedQuery<InflationRecord>(client, InflationRecord.class, "SELECT * FROM inflation WHERE year1 = 1998 AND year2 = ?",
				"DEMO", pdecls)){
			pq.setFetchSize(1);
			pq.setParameter(1, 2006);
			try(var rsINF = pq.createResultSet()){
				try {
					while (rsINF.next()) {
						var nextRow = (InflationRecord)(rsINF.get());
						Logging.log("year1 = %d, year2 = %d, factor = %f", nextRow.year1, nextRow.year2, nextRow.factor);
					}
				}catch(TWResultSetException rse) {rse = rse;}
			}
			/*var noMoreRows = new Object();
			var controller = new NotificationResultController<PubmedRecord>(PubmedRecord.class,  pq.getFetchSize(), null, null) {
				@Override
				public boolean handleRows(List<PubmedRecord> rows, boolean exhausted) {
					for (var pm : rows) {
						Logging.log("notification of PubmedRecord: id = %s, title = %s", pm.id, pm.title);	}
					if (exhausted) {
						Logging.log("no more notifications of PubmedRecord");	
						synchronized(noMoreRows) {noMoreRows.notify();}
						return false;
					}
					return true;
				}};
			pq.setParameter(1, "%o%");
			pq.executeQueryWithNotificationResults(controller);
			synchronized(noMoreRows) {try {noMoreRows.wait();
			} catch (InterruptedException e) {}}*/
		}
	}
	

	private static void testPolledQueries(TriggerwareClient client) 
			throws JRPCException, TriggerwareClientException {
		var pollingDone = new Object();
		var pqt1 = new PolledTimestampQuery(client, 5, pollingDone);
		pqt1.register();
		pqt1.poll();
		synchronized(pollingDone) {try {pollingDone.wait(); pqt1.close();
		} catch (InterruptedException e) {}};

		var schedule = new PolledQueryPeriodicSchedule(Duration.ofSeconds(10));
		var pqt2 = new ScheduledTimestampQuery(client, schedule);
		pqt2.register();
		pqt2.activate();
		try {Thread.sleep(60*1000);//1 minute
		} catch (InterruptedException e) {}
		pqt2.close();
		

		/*var pqd = new ScheduledDurationQuery(client, schedule);
		pqd.register();
		pqd.activate();
		try {
			Thread.sleep(3*60*1000);//3 minutes
		} catch (InterruptedException e) {
		}
		pqd.close();*/
	}
	/*private static PolledQuery<int[]> polledQueryTest(TriggerwareClient client) 
			throws JRPCException, TriggerwareClientException {
		//var schedule = new PolledQueryPeriodicSchedule(Duration.ofSeconds(5));
		var schedule = new PolledQueryCalendarSchedule();
		try {
			schedule.setHours("*").setMinutes("*").setDays("1-31");
		} catch (ScheduleFormatException e1) {	}
		var pq = new SamplePolledQuery(schedule, client.getPrimaryConnection());
		pq.activate();
		try {
			//Thread.sleep(30000); // 30 seconds for periodic
			Thread.sleep(10*60*60*1000); // 10 minutes for calendar
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			
		}
		return pq;
	}*/
	public static class TemporalFields {
		  @JsonProperty("ldt")
		  private Instant instant = Instant.now();

		  @JsonProperty("ld")
		  private LocalDate localDate= LocalDate.of(1947,6,13);

		  @JsonProperty("lt")
		  private LocalTime localTime= LocalTime.NOON.plusMinutes(15).plusSeconds(23);
		  
		  @JsonProperty("dur")
		  private Duration duration = Duration.ofDays(2).plusHours(11).plusMinutes(15).plusSeconds(27).plusNanos(1000*1000*492 +722);
		  @DeserializationConstructor
		  public TemporalFields(Instant i, LocalDate ld, LocalTime lt, Duration dur){
			  instant = i; localDate = ld; localTime = lt; duration = dur;
		  }
	}
	
	/*private static void testTime() {
		String jsonString = "\"2024-04-13T13:30:00Z\"";

        var mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
        //objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		try {
	        var jParser = mapper.createParser(jsonString);
	        var instant =  mapper.readValue(jsonString, Instant.class);
	        instant = instant;
		} catch (Exception e) {
			e = e;		}

	}*/

	//to debug TW server, telnet localhost 8226..When done, enter the zero (0) command to leave the telnet session
	public static void main(String[] args) throws Exception {

		Logging.setLogger(new ILogger() {
			@Override
			public synchronized void log (String event) {System.out.println(event);}
			@Override
			public synchronized void log (Throwable t, String event) {
				System.err.println(t.getMessage());
				System.err.println(event);
				}
			@Override
			public synchronized void log (String format, Object ... args) {System.out.println(String.format(format,args));}
		});

		//var typeRef = new TypeReference<ResultSetResult<Object[]>>() {};
        
		/*parserTest(typeRef, """
				{"signature" : [{"attribute": "FACTOR","type" : ""}], 
				 "batch": {"count" : 1, "exhausted" : true, "tuples" : [[1.12]]}}"
				""");*/
		/*parserTest( PreparedQuery.PreparedQueryRegistration.class, """
				{"signature":[{"attribute":"year1","type":"smallInt"},{"attribute":"year2","type":"smallInt"},{"attribute":"factor","type":"Float"}],
				 "handle":1,
				 "inputSignature":[{"attribute":"?1","type":"int"}],
				 "usesNamedParameters":false}
				""");*/
		

		/*var jfactory = new MappingJsonFactory();
		JsonParser jParser = jfactory.createParser("[ [32.5] , [44] ] ");
		var javaVal = jParser.readValueAs(java.util.ArrayList[].class);
		int n = javaVal.length;*/

		var twClient = new TestClient(InetAddress.getLoopbackAddress(),5221);
		//testAdhocQueries(twClient);
		//testPreparedQueries(twClient);
		SwamyRegression.swamyRegressionQueries(twClient);
		//testPolledQueries(twClient);
		
		//var tpq = polledQueryTest(twClient);
		//tpq.close);
		//PerformanceTesting.testPreparedQuery(twClient);
	}

    /*static void marketData(String tickerSymbol) throws IOException, InterruptedException {
    	var client =  java.net.http.HttpClient.newHttpClient();
    	var suri = //"https://yh-finance.p.rapidapi.com/market/v2/get-quotes?region=US&f=ab&symbols=" +
    			   "https://query1.finance.yahoo.com/v7/finance/quote?lang=en-US&region=US&corsDomain=finance.yahoo.com&symbols=" +
    	        		   URLEncoder.encode(tickerSymbol, "UTF-8");
		var request = java.net.http.HttpRequest.newBuilder()
				.uri(java.net.URI.create(suri))
				//.header("x-rapidapi-host", "yh-finance.p.rapidapi.com")
				//.header("x-rapidapi-key", "a5c561ec3cmshafa687a33b07085p17e1adjsn3792ffc8b5d6")
				.method("GET", java.net.http.HttpRequest.BodyPublishers.noBody())
				.build();
		//var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
		client.sendAsync(request, java.net.http.HttpResponse.BodyHandlers.ofString())
        .thenApply(java.net.http.HttpResponse::body)
        .thenAccept((json) -> {System.out.println(json); System.out.flush();})//System.out::println)
        .join();
    }*/

}
