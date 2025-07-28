package calqlogic.twservercomms;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonProperty;


//@JsonIgnoreProperties(ignoreUnknown = true) //until this is stable and I know there are no additional properties in the serialization
public class RowsDelta<T> {

	@JsonProperty("added")
	private ArrayList<T> added;
	@JsonProperty("deleted")
	private ArrayList<T> deleted;
	
	public RowsDelta() {} //for default jackson deserializer
	
	public ArrayList<T>getAdded(){return added;}
	public ArrayList<T>getDeleted(){return deleted;}
}
