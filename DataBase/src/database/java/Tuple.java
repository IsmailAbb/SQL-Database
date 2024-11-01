package database.java;
import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable{
	
	Hashtable<String, Object> ColNameValue;
	Object primaryKey;
	String pkName;
	String pkType;
	
	public Tuple(Hashtable<String, Object> ColNameValue, String pkName, Object pk, String pkType) {
		this.ColNameValue = ColNameValue;
		this.primaryKey = pk;
		this.pkName = pkName;
		this.pkType = pkType;
	}
	public String toString()
	{
	 return	ColNameValue.toString() + '\n';
		
	}
	
	public boolean equals(Tuple t) {
		return this.primaryKey.equals(t.primaryKey);
	}
}
