package calqlogic.twservercomms;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.*;

@JsonFormat(shape=JsonFormat.Shape.OBJECT)
class Batch<T>{

	//private final int count;
	private final /*BatchRows*/ ArrayList<T> rows;
	private final boolean exhausted;
	@JsonCreator 
	public Batch(@JsonProperty("count") int count, @JsonProperty(value="tuples", required=false) BatchRows<T> rows, 
			@JsonProperty(value="exhausted",required=false)Boolean exhausted){
		 //this.count = count;
		 this.rows = rows==null ? null : rows.getRows() ;
		 if (exhausted != null)
			 this.exhausted = exhausted;
		 else this.exhausted = false;
	 }
	
	//public int getCount() {return count;}
	public ArrayList<T>getRows(){ return rows;} //return rows == null ?  null : rows.getRows();}
	public boolean isExhausted() {return exhausted;}
}
