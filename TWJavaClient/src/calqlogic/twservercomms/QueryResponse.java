package calqlogic.twservercomms;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFormat(shape=JsonFormat.Shape.OBJECT)
class QueryResponse<T> {
	private String completionStatus;
	private ArrayList<T> rows;
	private String[] columns;
	
	@JsonCreator
	public QueryResponse(@JsonProperty("result")ArrayList<T> rows, @JsonProperty("columns")String[]columns,
						@JsonProperty("exitstatus")String cs){
		this.completionStatus = cs;
		this.columns = columns;
		this.rows = rows;
	}

	//@JsonProperty("exitstatus")
	public String getCompletionStatus() {return completionStatus;}
	//@JsonProperty("result")
	public ArrayList<T> getRows() {return rows;}
	public String[] getColumns() {return columns;}
}
