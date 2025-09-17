package calqlogic.twservercomms;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import calqlogic.twservercomms.Test.TestClient;
import calqlogic.twservercomms.TriggerwareClient.TWRuntimeMeasure;
import nmg.softwareworks.jrpcagent.JRPCException;
import nmg.softwareworks.jrpcagent.Logging;

class PerformanceTesting {
	private interface TimeTestCode {
	    public void execute() throws Exception;
	}

	static long[] measurementTime = null;
	private static long[] timeTest(TriggerwareClient twClient, TimeTestCode toTime) {
		TWRuntimeMeasure origTime = null,finalTime = null;
		try {origTime = twClient.runtime();
		} catch (JRPCException e1) { return null;}
		
		try {toTime.execute();	
		}catch (Throwable t) {
			return null;}

		try {finalTime = twClient.runtime();
		} catch (JRPCException e1) {return null;}
		
		long runTime = finalTime.runTime-origTime.runTime;
		long gcTime = finalTime.gcTime-origTime.gcTime;
		long bytes = finalTime.bytes-origTime.bytes;
		long[] measured = new long[] {runTime-gcTime, bytes};
		if (measurementTime != null) {//subtract measurementTime
			measured[0] -= measurementTime[0];
			measured[1] -= measurementTime[1];
		}
		return measured;		
	}

	
	static long[] noopTimeTest(TestClient twClient, int n){
		long time=0,space=0;
		for (int i = n; i>0; i--) {
			long[] thisTime = timeTest(twClient, ()->{twClient.noop();});
			if (thisTime == null) return null;
			time += thisTime[0];  space += thisTime[1];
		}
		return new long[] {time/n, space/n};
	}
	
	private static long[] measurementTimeTest(TriggerwareClient twClient, int n) {
		long time=0,space=0;
		for (int i = n; i>0; i--) {
			long[] thisTime = timeTest(twClient, ()->{});
			if (thisTime == null) return null;
			time += thisTime[0];  space += thisTime[1];
		}
		measurementTime= new long[] {time/n, space/n};
		return measurementTime;
	}
	

	/*private static long[] logTimeTest(int n, String msg) {long time=0,space=0;
		for (int i = n; i>0; i--) {
			long[] thisTime = timeTest( ()->{TriggerwareClient.log(msg);});
			time += thisTime[0];  space += thisTime[1];
		}
		return new long[] {time/n, space/n};
	}*/
	
	private static PreparedQuery<?> testPreparedQuery = null;
	private static long[] pqTimeTest(TriggerwareClient twClient) {
		var pdecls = new PreparedQuery.NamedParameterDeclarations(); pdecls.put("col1Min", "integer"); pdecls.put("col2Max", "integer");
		return timeTest(twClient, ()->{
		  testPreparedQuery=
				new PreparedQuery<IntegerPair>(twClient, IntegerPair.class, "SELECT * FROM r2test WHERE col1 >=:col1Min AND col2<=:col2Max", 
						  "AP5", pdecls);});
	}
	
	@JsonFormat(shape=JsonFormat.Shape.ARRAY)
	@JsonPropertyOrder({ "first",  "second" }) 
	static class IntegerPair{
		private int first;
		private int second;
		public int getFirst() {return first;}
		public int getSecond() {return second;}
	}
	
	/*private static void debugTest(TriggerwareClient twClient) throws JRPCException, TriggerwareClientException {
		  var qs = twClient.createQuery();
		  qs.setFetchSize(null);
		  try (var rs = qs.executeQuery(IntegerPair.class, "SELECT * FROM r2test WHERE col1 >=11 AND col2<=15", "AP5")){
			 int seen = 0;
		     while (rs.next()) {
		    	IntegerPair ip = (IntegerPair)rs.get(); 
		        seen++;
		     }
		     seen = seen;
		  } catch (Exception e) {
		}
		  
		  PreparedQuery<IntegerPair>  pq = new PreparedQuery<>(IntegerPair.class, "SELECT * FROM r2test WHERE col1 >=:col1Min AND col2<=:col2Max",
				  "AP5",   twClient);
		  pq.setFetchSize(null);
		  pq.clearParameters();
		  pq.setParameter("col1Min", 11);
		  pq.setParameter("col2Max", 15);
		  try (var rs = pq.executeQuery()){
			 int seen = 0;
		     while (rs.next()) {
		    	var tuple=rs.get(); 
		        seen++;
		     }
		     seen = seen;
		  }catch(TimeoutException tex) {}
	}*/
	
	private static long[] pqcTimeTest(TriggerwareClient twClient) {
		return timeTest(twClient, ()->{testPreparedQuery.close();});}

	private static <T> long[] pqUsageTest(TriggerwareClient twClient, int howmany, PreparedQuery<T> pq, int c1min, int c2max) {
		return timeTest(twClient, ()->{
			  pq.setFetchSize(null);
			  for (int count=howmany; count>0;  count--) {
				  pq.clearParameters();
				  pq.setParameter("col1Min", c1min);
				  pq.setParameter("col2Max", c2max);
				  try (var rs = pq.createResultSet()){
					 int seen = 0;
				     while (rs.next()) {
				    	var tuple=rs.get(); 
				        seen++;
				     }
				     seen = seen;
				  }
			  }
		});
	}

	/*private static long[] askqtimeTestFol(TriggerwareClient twClient, int howmany, int c1min, int c2max) {
		return timeTest(twClient, ()->{
			for (int count = howmany; count>0;  count--) {
				String query = String.format("((col1 col2) s.t. (and (r2test col1 col2)(>= col1 %d) (<= col2 %d)))", c1min, c2max);
				var params = new NamedRequestParameters(). with("query", query) .with ("lang", Language.FOL) .with("schema", "AP5");
				var qr = twClient.askQuery(params, IntegerPair.class);
				var tuples = qr.getTuples();
				int n = tuples.size();
				//var js = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(qr);
				//js = js;
			  }			
		  });
	}

	private static long[] askqtimeTestSql(TriggerwareClient twClient, int howmany, int c1min, int c2max) {
		return  timeTest(twClient, ()->{
			for (int count = howmany; count>0;  count--) {
				String query = String.format("SELECT * FROM r2test WHERE col1 >= %d AND col2<=%d", c1min, c2max);
				var askqParms = new NamedRequestParameters(). with("query", query) .with ("lang", Language.SQL) .with("schema", "AP5");
				var qr = twClient.askQuery(askqParms, IntegerPair.class);
				var tuples = qr.getTuples();
				int n = tuples.size();
			  }
		});
	}*/

	/*static PositionalParameterRequest<Object[]> requestDataRequest = 
			new PositionalParameterRequest<Object[]>(Object[].class, false, "get-request-data", 0, 0);
	static PositionalParameterRequest<Object[][]> poiRequest = 
			new PositionalParameterRequest<Object[][]>(Object[][].class, false, "get-all-areas", 0, 0);*/
	public static void testPreparedQuery(TestClient twClient) {//throws JRPCException, MiddlewareException, InterruptedException, ExecutionException {
		/*try{
			var areas = twClient.execute(poiRequest);
			areas = areas;
		}catch(JRPCException e) {
			e = e;
		}*/
		//askqtimeTestFol(twClient, 1, 11, 15); //let ap5 cache the query plan
		var origLogger = Logging.setLogger(Logging.getEmptyLogger());
		//askqtimeTestSql(twClient, 1, 11, 15);  //let ap5 cache the query plan and the sql translator cache the signature
		Logging.setLogger(origLogger);
		/*try {
			debugTest(twClient);
		} catch (JRPCException | TriggerwareClientException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		measurementTimeTest(twClient, 20);
		long[] noopTime = noopTimeTest(twClient, 20);
		Logging.setLogger(origLogger);
		if (noopTime != null)
			Logging.log("pairs are [usec, bytes]. measurement = [%d,%d] noop = [%d,%d]  %n",
	         measurementTime[0], measurementTime[1], noopTime[0], noopTime[1]);
		for (int repeatCount : new int[] {5,30,100}) {
			origLogger = Logging.setLogger(Logging.getEmptyLogger());
			/*long[] askFoltime = askqtimeTestFol(twClient, repeatCount, 11, 15);
			if (askFoltime == null) break;
			long[] askSqltime = askqtimeTestSql(twClient, repeatCount, 11, 15);
			if (askSqltime == null) break;*/
			long[] pqtime = pqTimeTest(twClient);
			if (pqtime == null) break;
			long[] pqutime = pqUsageTest(twClient, repeatCount, testPreparedQuery,  11, 15);
			if (pqutime == null) break;
			long[] pqctime = pqcTimeTest(twClient);
			if (pqctime == null) break;
			Logging.setLogger(origLogger);
			Logging.log("pairs are [usec, bytes]. " + //Fol = [%d,%d]  Sql = [%d,%d]  " + 
			              "preparing = [%d,%d] generating = [%d,%d] closing = [%d,%d]%n",
			         //askFoltime[0], askFoltime[1], askSqltime[0], askSqltime[1],
				     pqtime[0],pqtime[1], pqutime[0],pqutime[1], pqctime[0],pqctime[1]);
		}
		Logging.setLogger(origLogger);
	}
}
