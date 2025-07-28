package nmg.softwareworks.jrpcagent;

import java.util.HashMap;


/**
 * NamedRequestParameters are used to supply parameters for server requests that expect
 * parameters to be supplied as a Json object.
 * @author nmg
 *
 */
public class NamedRequestParameters extends HashMap<String, Object> {
	/**
	 * create a new NamedRequestParameters with a default initial storage allocation
	 */
	public NamedRequestParameters() {super();}
	/**
	 * @param n an estimate of the number of parameters, used for initial storage allocation. The new instance
	 * may eventually have more or fewer parameters.
	 */
	//public NamedRequestParameters(int n) {super(n);}
	/**
	 * @param paramName the name of parameter to add
	 * @param paramValue the value for the parameter
	 * @return this NamedRequestParameters instance. This method makes it convenient to chain the provision of named parameters.
	 */
	public NamedRequestParameters with(String paramName, Object paramValue) {
		assert paramName!=null;
		this.put(paramName, paramValue);//JsonUtilities.serialize(paramValue));
		return this;
	}

	public void validate(OutboundRequest<?> request, String[]requiredParameterNames, String[]optionalParameterNames) {
				//throws ActualParameterException{
		if (requiredParameterNames != null) {
			for (String req :requiredParameterNames) {
				if (!containsKey(req)) 
					throw new JRPCRuntimeException.ActualParameterException(String.format("missing required parameter name %s", req));
			}
		}
		outer:
		for (String s : keySet()) {
			if (requiredParameterNames != null)
				for (var p : requiredParameterNames)
					if (s.equals(p)) continue outer;
			if (optionalParameterNames != null)
				for (var p : optionalParameterNames)
					if (s.equals(p)) continue outer;
			throw new JRPCRuntimeException.ActualParameterException(String.format("supplied unknown parameter name %s", s));
		}
	}
}
