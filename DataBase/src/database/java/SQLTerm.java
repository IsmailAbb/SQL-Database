package database.java;

public class SQLTerm {
	String _strTableName;
	String _strColumnName;
	String _strOperator;
	Object _objValue;
	
	
	public SQLTerm(String tableName, String columnName, String operator, Object objValue) {
		this._strTableName = tableName;
		this._strColumnName = columnName;
		this._strOperator = operator;
		this._objValue = objValue;
	}
	
}
