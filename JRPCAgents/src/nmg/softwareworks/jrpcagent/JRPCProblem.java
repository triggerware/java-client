package nmg.softwareworks.jrpcagent;


public interface JRPCProblem {
   int getCode();
   String getMessage();
   Object getData();
}
