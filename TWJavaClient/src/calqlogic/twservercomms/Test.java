package calqlogic.twservercomms;


import java.io.*;
import java.net.InetAddress;
import java.time.*;


import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.*;
import calqlogic.twservercomms.PolledQueryCalendarSchedule.ScheduleFormatException;
import calqlogic.twservercomms.TriggerwareClient.TriggerwareClientException;
import nmg.softwareworks.jrpcagent.*;
import nmg.softwareworks.jrpcagent.Logging.ILogger;


class Test {
	
	static class TestClient extends TriggerwareClient {

		public TestClient(InetAddress twHost, int twServerPort) throws Exception {
			super("test client", twHost, twServerPort);	}
		
	}

	/*static String sample = 
			"{\"jsonrpc\":\"2.0\",\"id\":5,\"result\":{\"batch\":{\"count\":4,\"exhausted\":true,\"tuples\":[[14,15],[13,14],[12,13],[11,12]]}}}  []";
	

	private static void parseTest(String json) 
			throws JsonParseException, IOException {
		var input = isFromString(json); //new java.io.ByteArrayInputStream(json.getBytes());
		var fromTW = new JsonUtilities.TWStreamParser(input); 
		fromTW.startLogging();
		var msg = fromTW.next();
		var msgText = fromTW.logEntryComplete();
		var rslt = msg.get("result");
		TWResultSet rs = null;
		TWResultSet.Batch.fromJson((ObjectNode)rslt, rs);
	}*/
	/*private static String serializeTest() throws IOException {
		 var jfactory = new MappingJsonFactory();
		 var baos = new java.io.ByteArrayOutputStream();
		 var gen = jfactory.createGenerator(baos);
		 var om = new ObjectMapper(jfactory);
		om.writeValue(gen, new PolledQuerySchedule());
		result.add(new Object[] {"a", 1});
		result.add(new Object[] {"b", 2});
		result.add(new Object[] {"c", 3});
		var qr = new QueryResponse<Object[]>( result, new String[]{"c1", "c2"}, "done");
		om.writeValue(gen, qr);
		//om.writeValue(gen, JRPCRequest.class);
		var json = baos.toString();
		return json;		
	}*/
	/*private static class Address {
		//@JsonProperty("streetNumber")
		private int streetNumber;
		//@JsonProperty("streetName")
		private String streetName;
		@JsonCreator 
		public Address(@JsonProperty("streetName") String name, @JsonProperty("streetNumber") int n){
	      this.streetName = name; 
	      this.streetNumber = n; 
		}
	}*/

	@JsonFormat(shape=JsonFormat.Shape.ARRAY)
	/*@JsonPropertyOrder({ "age",  "address", "name", "nicks" })  //the braces are not Json -- they construct an array param value for a Java annotation
	private static class Person{
		private int age;
		private String name;
		private ArrayList<String> nickNames;
		private Address address;
		public int getAge() {return age;}
		public String getName() {return name;}
		public String getAddress() {return name;}
		public ArrayList<String>getNickNames(){return nickNames;}
	}*/
	/*private static ArrayList[] ArrayOfArrayListDeserializeTest() throws  IOException {
		var jfactory = new MappingJsonFactory();
		JsonParser jParser = jfactory.createParser("[ [32.5] , [44] ] ");
		var javaVal = jParser.readValueAs(java.util.ArrayList[].class);
		return javaVal;
	}*/
	/*public static class ClassDeserializer extends StdDeserializer<Class> {

	    public ClassDeserializer(Class vc) {      super(vc);   }

	    @Override
	    public Class<?> deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {	    	
	        if (JsonToken.VALUE_STRING == parser.currentToken()){
	        	var typeName = parser.getText();
				try {
					var klass = Class.forName(typeName);
					return klass;
				} catch (ClassNotFoundException e) {return null;}
	        }
	        return null;
	    }
	}*/
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
	/*private static Class classDeserializeTest() throws  IOException {
		//var module =  new SimpleModule("ClassDeserializer");
		//module.addDeserializer(Class.class, new ClassDeserializer(Class.class));
		var mapper = new ObjectMapper();
		//mapper.registerModule(module);
		var jfactory = new MappingJsonFactory();
		var jParser = jfactory.createParser("\"calqlogic.twservercomms.JRPCRequest\"");
		var klass = mapper.readValue(jParser,Class.class);
	    return klass;
	}*/
	/*private static Person tupleDeserializeTest() throws JsonParseException, IOException { 
		String json = "[ 33, {\"streetName\" : \"Washington\", \"streetNumber\" : 378}, \"Joseph\", [\"Joey\"] ] ";
		var jfactory = new MappingJsonFactory();
		var jParser = jfactory.createParser(json);				
		var ip = jParser.readValueAs(Person.class);
		// now try using a type reference
		jParser = jfactory.createParser(json);
		ip = jParser.readValueAs(new TypeReference<Person>() {});
		return ip;
	}*/
	/*
	@JsonFormat(shape=JsonFormat.Shape.ARRAY)
	@JsonPropertyOrder({"calls"})
		//,  "categories", "resolutions", "locations", "counters" })  //the braces are not Json -- they construct an array param value for a Java annotation
	private static class RequestData{
		public Object[] calls;
		private String[] categories;
		private String[] resolutions;
		private String[] locations;
		private Object[] counters;
		public Object[] getCalls() {return calls;}
		public String[] getCategories(){return categories;}
		public String[] getResolutions() {return resolutions;}
		public String[] getLocations() {return locations;}
		public Object[] getCounters() {return counters;}
	}
	private static RequestData tupleDeserializeTest2() throws JsonParseException, IOException{
		String json = "[null," +
             	 "[\"Cement\",\"Food\",\"Furniture\",\"Insulin\",\"Vans\",\"Water\"], " +
           	 "[\"Denied\",\"Dispatched\",\"Outsourced\"]," + 
           	 "[\"Abandoned Mill\",\"Cedars Sinai\",\"Founders Bank\",\"Fox Hills Mall\",\"Mayfield High\",\"Museum\"], " +
           	 "[[\"REQUEST-COUNT-OVERALL\",0],[\"UNRESOLVED-REQUEST-COUNT\",0],[\"HOSPITAL-SURGE-COUNT\",0],[\"SURGE-COUNT\",0],[\"LATE-RESOLUTIONS-COUNT\",0]]]";
           	 
		var jfactory = new MappingJsonFactory();
		var jParser = jfactory.createParser(json);
		var javaVal = jParser.readValueAs(RequestData.class);
		return javaVal;
	}*/
	
	private static InputStream isFromString(String json) {
        return new ByteArrayInputStream(json.getBytes());}
	
	/*private static String serializeTest(Object v) throws IOException {
		 var jfactory = new MappingJsonFactory();
		 var baos = new java.io.ByteArrayOutputStream();
		 var gen = jfactory.createGenerator(baos);
		 var om = new ObjectMapper(jfactory);
		om.writeValue(gen, v);
		var json = baos.toString();
		return json;		
	}*/
	/*private static QueryStatement.ExecuteQueryResult<Address> executeQueryDeserializeTest() throws JsonParseException, IOException {
		var jfactory = new MappingJsonFactory();
		var om = new ObjectMapper(jfactory);
		JsonParser jParser = jfactory.createParser(
				"{\"handle\" : null, \"tuples\": [{\"streetName\" : \"Washington\", \"streetNumber\" : 378}]} ");

		//var jt = om.getTypeFactory().constructParametricType(QueryStatement.ExecuteQueryResult.class, Address.class);
		
		//var xx = ( QueryStatement.ExecuteQueryResult<Address>)om.readValue(jParser,jt)	;
		var xx = (QueryStatement.ExecuteQueryResult<Address>)jParser.readValueAs(new TypeReference<QueryStatement.ExecuteQueryResult<Address>>() {});
		return xx;
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
	static void testPreparedQuery(TriggerwareClient twc) throws JRPCException, TriggerwareClientException {
		var query = "select * from bizfilefind, bizfiledetails where searchfor= :company and bizfilefind.id = bizfiledetails.id";
		var pq = new PreparedQuery(Object[].class,query, Language.SQL,"AP5", twc);
		pq.setParameter("company", "ibm");
		var rs = pq.executeQuery();
		rs = rs;
	}
	/*private static TWResultSet<Object[]> testPreparedQuery(TriggerwareClient client) throws JRPCException, TriggerwareClientException{
		var pq = new PreparedQuery<Object[]>(Object[].class, 
				"select * from [csimarket-competition] where  companysymbol = 'AMGN'", 
				 "AP5", client);
		var rs = pq.executeQuery();
		return rs;
	}*/
	/*private static void testObjectDeserialization() throws IOException {
		var jfactory = new MappingJsonFactory();
		var jParser = jfactory.createParser("true");
		var xxx = jParser.readValueAs(Object.class);
		jParser = jfactory.createParser("null");
		xxx = jParser.readValueAs(Object.class);
		jParser = jfactory.createParser("-44");
		xxx = jParser.readValueAs(Object.class);
		jParser = jfactory.createParser("-44");
		xxx = jParser.readValueAs(Long.class);
		jParser = jfactory.createParser("-44");
		xxx = jParser.readValueAs(Float.TYPE);
		jParser = jfactory.createParser("-44");
		xxx = jParser.readValueAs(Double.TYPE);
		jParser = jfactory.createParser("37.8");
		xxx = jParser.readValueAs(Object.class);
		jParser = jfactory.createParser("37.8");
		xxx = jParser.readValueAs(Integer.class);
		jParser = jfactory.createParser("\"abc\"");
		xxx = jParser.readValueAs(Object.class);
		jParser = jfactory.createParser("[1,2,3]");
		xxx = jParser.readValueAs(Object.class);
		jParser = jfactory.createParser("[1,2,3]");
		xxx = jParser.readValueAs(Object[].class);
		jParser = jfactory.createParser("[[1],[2],[3]]");
		xxx = jParser.readValueAs(Object[][].class);
		return;
	}*/
	
	private static class SamplePolledQuery extends ScheduledQuery<int[]> { 
		//static TypeReference<PolledQueryNotification<int[]>> tr = new TypeReference<PolledQueryNotification<int[]>>() {};
		
		SamplePolledQuery(PolledQuerySchedule schedule, TriggerwareConnection connection) throws JRPCException{
			super(int[].class, schedule, "((n)s.t.(volatile n))", Language.FOL, "AP5",connection,
					new PolledQueryControlParameters(true, true, null));}

		@Override
		public void handleSuccess(RowsDelta<int[]> delta, Instant ts) {
			Logging.log("SamplePolledQuery reports change as of %s.%n added=%s deleted=%s", 
					ts, delta.getAdded(), delta.getDeleted());}
		@Override
		public void handleError(String errorText, Instant ts) {//just log
			super.handleError(errorText, ts);	}
	}
	private static QueryStatement simpleQueryTest(TriggerwareClient client) 
			throws JRPCException, TriggerwareClientException {
		var qs = new QueryStatement(client);
		qs.setFetchSize(10);
		var sql = //"SELECT symbol FROM StockSymbol WHERE exchange = 'NASDAQ'";
				"select * from device where MANUFACTURER_D_NAME like '%MEDTRONIC%' ";
		var rs = qs.executeQuery(sql, "AP5");		
		return qs;
	}
	private static PolledQuery<int[]> polledQueryTest(TriggerwareClient client) 
			throws JRPCException, TriggerwareClientException {
		//var schedule = new PolledQueryPeriodicSchedule(Duration.ofSeconds(5));
		var schedule = new PolledQueryCalendarSchedule();
		try {
			schedule.setHours("*").setMinutes("*").setDays("1-31");
		} catch (ScheduleFormatException e1) {	}
		/*var baos = new java.io.ByteArrayOutputStream();
		try {
			client.serialize(schedule, baos);
			var json = baos.toString();
			return null;
		} catch (IOException e1) {}*/
		var pq = new SamplePolledQuery(schedule, client.getPrimaryConnection());
		//pq.register(client);
		pq.activate();
		try {
			//Thread.sleep(30000); // 30 seconds for periodic
			Thread.sleep(10*60*60*1000); // 10 minutes for calendar
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			
		}
		return pq;
	}
	//to debug TW server, telnet localhost 8226..When done, enter the zero (0) command to leave the telnet session
	public static void main(String[] args) throws Exception {
		//marketData("^GSPC");
		/*var d = Double.MAX_VALUE;
        d = Double.MAX_VALUE + 1.0;
        d = 50.0 + Double.MAX_VALUE ;
        d = Double.MAX_VALUE + Double.MAX_VALUE;*/
		Logging.setLogger(new ILogger() {
			@Override
			public synchronized void log (String event) {System.out.println(event);}
			@Override
			public synchronized void log (Throwable t, String event) {
				System.err.println(t.getMessage());
				System.err.println(event);
				}
			@Override
			public synchronized void log (String format, Object ... args) {System.out.printf((format) + "%n",args);}
		});
		//var json = serializeTest();
		//var aa = ArrayOfArrayListDeserializeTest();
		//Person ppp = tupleDeserializeTest();
		//String sss = serializeTest(ppp);
		//var xxx = executeQueryDeserializeTest();
		//var xxx = queryResponseDeserializeTest();
		//classDeserializeTest();
		//testObjectDeserialization();
		/*var jfactory = new MappingJsonFactory();
		JsonParser jParser = jfactory.createParser("[ [32.5] , [44] ] ");
		var javaVal = jParser.readValueAs(java.util.ArrayList[].class);
		int n = javaVal.length;*/
		//var tpl = JsonUtilities.deserializeArrayAsTuple(jParser, new Class<?>[] {Double.TYPE, Double.TYPE});
		//var tkn = jParser.currentToken();
		var twClient = new TestClient(InetAddress.getLoopbackAddress(),5221);
		testPreparedQuery(twClient);
		/*while (true) {
			try{
				var tsq = simpleQueryTest(twClient);
				tsq.close();
				break;
			}catch (Throwable t) {
				t=t;
			}
		}*/
		var tpq = polledQueryTest(twClient);
		tpq.closeQuery();
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
