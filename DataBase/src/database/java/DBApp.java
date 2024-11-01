package database.java;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Vector;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

public class DBApp {
	
	public DBApp() {
	}

	public void init() {
		File tableDir = new File("./src/main/resources/data");
		tableDir.mkdir();
		
	}

	public Vector<Tuple> selecFromResult(SQLTerm s, Vector<Tuple> resultList) throws DBAppException {
		 String colName = s._strColumnName;
		 String operator = s._strOperator;
		 Object value = s._objValue;
		 Vector<Tuple> result = new Vector<>();
		
		 switch(operator) {
		 case "=":
				 for(Tuple t : resultList) {
					 //checks
					 Object valueInTuple = t.ColNameValue.get(colName);
					 if(valueInTuple.equals(value)) {
						 result.add(t);
					 } 
				 }
			 			 
			 break;
		 case "!=":
				 for(Tuple t : resultList) {
					 //checks
					 Object valueInTuple = t.ColNameValue.get(colName);
					 if(!valueInTuple.equals(value)) {
						 result.add(t);
					 }
			 }
			 break;
		 case ">":
				 for(Tuple t : resultList) {
					 //checks
					 Object valueInTuple = t.ColNameValue.get(colName);
					 String type = valueInTuple.getClass().getName();
					 switch(type) {
					 case "java.lang.Integer":
						 if(((Integer)valueInTuple).compareTo((Integer)value)>0) {
							 result.add(t);
							
						 } break;
						 
					 case"java.lang.Double":
						 if(((Double)valueInTuple).compareTo((Double)value)>0) {
							 result.add(t);
							
						 }
						 break;
					 case "java.lang.String":
						 if(((String)valueInTuple).compareTo((String)value)>0) {
							 result.add(t);
							 
						 } break;
						default: break; 
					 } 
					
				 }
			 
			 break;
		 case "<":
				 for(Tuple t : resultList) {
					 //checks
					 Object valueInTuple = t.ColNameValue.get(colName);
					 String type = valueInTuple.getClass().getName();
					 switch(type) {
					 case "java.lang.Integer":
						 if(((Integer)valueInTuple).compareTo((Integer)value)<0) {
							 result.add(t);
						 }
						 break;
						 
					 case"java.lang.Double":
						 if(((Double)valueInTuple).compareTo((Double)value)<0) {
							 result.add(t);
							 
						 } break;
						 
					 case "java.lang.String":
						 if(((String)valueInTuple).compareTo((String)value)<0) {
							 result.add(t);
							
						 }  break;
						 default: break;
					 } 
			 }
			 break;
		 case ">=":
				 for(Tuple t : resultList) {
					 //checks
					 Object valueInTuple = t.ColNameValue.get(colName);
					 String type = valueInTuple.getClass().getName();
					 switch(type) {
					 case "java.lang.Integer":
						 if(((Integer)valueInTuple).compareTo((Integer)value)>=0) {
							 result.add(t);
							 
						 }  break;
						 
					 case"java.lang.Double":
						 if(((Double)valueInTuple).compareTo((Double)value)>=0) {
							 result.add(t);
							 
						 }  break;
						 
					 case "java.lang.String":
						 if(((String)valueInTuple).compareTo((String)value)>=0) {
							 result.add(t);
							
						 }  break;
						 default: break;
					 } 
			 }
			 break;
		 case "<=":
				 for(Tuple t : resultList) {
					 //checks
					 Object valueInTuple = t.ColNameValue.get(colName);
					 String type = valueInTuple.getClass().getName();
					 switch(type) {
					 case "java.lang.Integer":
						 if(((Integer)valueInTuple).compareTo((Integer)value)<=0) {
							 result.add(t);
							 
						 }  break;
						 
					 case"java.lang.Double":
						 if(((Double)valueInTuple).compareTo((Double)value)<=0) {
							 result.add(t);
							 
						 }
						 break;
					 case "java.lang.String":
						 if(((String)valueInTuple).compareTo((String)value)<=0) {
							 result.add(t);
							
						 }  break;
						 default: break;
					 } 
			 }
			 break;
		 default: break;
		 
	 }
		 return result;
		
		
	 }
	
    public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		Vector<Tuple> result1 = new Vector<>();
		Vector<Tuple> result2 = new Vector<>();
		SQLTerm s = arrSQLTerms[0];
		if (!tableExists(s._strTableName)) {
            throw new DBAppException("Table " + s._strTableName + " does not exist");
        }
		Table table = Table.deserialize(s._strTableName);	
		result1 =table.selectFromTable(s);
		int j = 0;
		for(int i = 1 ; i< arrSQLTerms.length; i++) {
			SQLTerm s1 = arrSQLTerms[i];
			String o = strarrOperators[j];
			j++;
			
			switch(o) {
			case "AND":
				result1 = selecFromResult(s1, result1);
				break;
				
			case "OR":
				result2 =table.selectFromTable(s1);
				
				for(Tuple tuple : result2) {
					result1.add(tuple);
				}
				break;
				
			case "XOR":
				
				result2 =table.selectFromTable(s1);
				
				result1 = xOR(result1, result2);
				break;
				
			default: break;	
			
			}
	}
		Table.serialize(table);
		return result1.iterator();
	}
	
	private Vector<Tuple> xOR(Vector<Tuple> result1, Vector<Tuple> result2) {
		Vector<Tuple> result = new Vector<>();
		
		for(Tuple tuple1: result1) {
			boolean flag1 = false;
			for(Tuple tuple2: result2) {
				flag1 = flag1 || tuple1.equals(tuple2);
				
			}
			if(!flag1) {
				result.add(tuple1);
			}
		}
		
		for(Tuple tuple2: result2) {
			boolean flag2 = false;
			for(Tuple tuple1: result1) {
				flag2 = flag2 || tuple1.equals(tuple2);
			}
			if(!flag2) {
				result.add(tuple2);
			}
		}
		return result;
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
            Hashtable<String,String> htblColNameType) throws DBAppException {
try {
// Check if the table already exists
if (tableExists(strTableName)) {
throw new DBAppException("Table " + strTableName + " already exists.");
}

// Create a new table directory
File tableDir = new File(strTableName);
if (!tableDir.mkdir()) {
throw new DBAppException("Failed to create table directory for " + strTableName);
}

// Write the table metadata to a metadata file
String filePath = "./src/main/resources/metadata.csv";

File metadataFile = new File(filePath);

// If metadata file doesn't exist, write the header row
if (!metadataFile.exists()) {
FileWriter fileWriter = new FileWriter(metadataFile);
BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
bufferedWriter.write("TableName,ColumnName,ColumnType,IsClusteringKey,IndexName,IndexType,ColumnMin,ColumnMax\n");
bufferedWriter.flush();
bufferedWriter.close();
}

// Write the column metadata
FileWriter fileWriter = new FileWriter(metadataFile, true); // append to existing file
BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

Enumeration<String> columnNames = htblColNameType.keys();
while (columnNames.hasMoreElements()) {
String columnName = columnNames.nextElement();
String columnType = htblColNameType.get(columnName);
String isClusteringKey = columnName.equals(strClusteringKeyColumn) ? "true" : "false";

bufferedWriter.write(strTableName + "," + columnName + "," + columnType + "," + isClusteringKey + "," + "null" + "," + "null" + ",null,null\n");
}

bufferedWriter.flush();
bufferedWriter.close();
Table table = new Table(strTableName);
Table.serialize(table);

} catch (IOException e) {
throw new DBAppException("Failed to create table " + strTableName + ": " + e.getMessage());
}
}

	private static boolean tableExists(String strTableName) {
	    File tableDir = new File("./src/main/resources/data/"+strTableName+".bin");
	    return tableDir.exists() ;
	}
	
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
			if (!tableExists(strTableName)) {
	            throw new DBAppException("Table " + strTableName + " does not exist");
	        }
				Table table = Table.deserialize(strTableName);
				table.insert(htblColNameValue);
				Table.serialize(table);
			}
	
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (!tableExists(strTableName)) {
           throw new DBAppException("Table " + strTableName + " does not exist");
    }
			Table table = Table.deserialize(strTableName);
				table.update(strClusteringKeyValue, htblColNameValue);
				Table.serialize(table);
				return;
			}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		if (!tableExists(strTableName)) {
	           throw new DBAppException("Table " + strTableName + " does not exist");
	           }
				Table table = Table.deserialize(strTableName);
				table.delete(htblColNameValue);
				Table.serialize(table);
				return;
			}
			
	public static void displayTable(String tableName) throws DBAppException {
			Table table = Table.deserialize(tableName);
			table.display();
			Table.serialize(table);
		}
		
	public static void createIndex(String strTableName, String strColName) throws DBAppException
		{
			if(!tableExists(strTableName))
			{
				throw new DBAppException("table does not exist");
			}
			Table table = Table.deserialize(strTableName);
			table.createIndex(strTableName, strColName);
			Table.serialize(table);
			
		}

	
//----------------------------------------------------------------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

        String strTableName = "test76";		
		DBApp dbApp = new DBApp( ); 
		dbApp.init();
		//Hashtable <String,String> htblColNameType = new Hashtable <>( ); 
		//htblColNameType.put("id", "java.lang.Integer"); 
		//htblColNameType.put("age", "java.lang.Integer"); 
		//htblColNameType.put("name", "java.lang.String"); 
		//htblColNameType.put("major", "java.lang.String"); 
		//htblColNameType.put("gpa", "java.lang.Double"); 
		//htblColNameType.put("phone", "java.lang.String"); 
		//htblColNameType.put("email", "java.lang.String"); 
		//htblColNameType.put("address", "java.lang.String"); 
		
		//try {
		//dbApp.createTable( strTableName, "id", htblColNameType);
	    //} catch (DBAppException e) {
		// TODO Auto-generated catch block
		//e.printStackTrace();
	    //}
		//Hashtable<String,Object> htblColNameValue = new Hashtable<>( ); 
		
		//Table tempTable = Table.deserialize(strTableName);

		/*htblColNameValue.put("id", 1);
		htblColNameValue.put("name", "Aml");
		htblColNameValue.put("major", "MET");
		htblColNameValue.put("age", 12);
		htblColNameValue.put("gpa", 0.7);
		htblColNameValue.put("phone", "111");
		htblColNameValue.put("email", "aaa");
		htblColNameValue.put("address", "aaa");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 2);
		htblColNameValue.put("age", 19);
		htblColNameValue.put("name", "Rowayda");
		htblColNameValue.put("major", "IET");
		htblColNameValue.put("gpa", 0.8);
		htblColNameValue.put("phone", "222");
		htblColNameValue.put("email", "bbb");
		htblColNameValue.put("address", "bbb");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 3);
		htblColNameValue.put("age", 30);
		htblColNameValue.put("name", "Rotana");
		htblColNameValue.put("major", "IET");
		htblColNameValue.put("gpa", 0.8);
		htblColNameValue.put("phone", "333");
		htblColNameValue.put("email", "ccc");
		htblColNameValue.put("address", "ccc");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 4);
		htblColNameValue.put("age", 25);
		htblColNameValue.put("name", "Farah");
		htblColNameValue.put("major", "DMET");
		htblColNameValue.put("gpa",  0.7);
		htblColNameValue.put("phone", "444");
		htblColNameValue.put("email", "ddd");
		htblColNameValue.put("address", "ddd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);	
		htblColNameValue.clear();
		htblColNameValue.put("id", 5);
		htblColNameValue.put("age", 35);
		htblColNameValue.put("name", "Rana");
		htblColNameValue.put("major", "DMET");
		htblColNameValue.put("gpa",  1.0);
		htblColNameValue.put("phone", "555");
		htblColNameValue.put("email", "dde");
		htblColNameValue.put("address", "dedd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 6);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 7);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 8);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 9);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 10);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 11);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 12);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 13);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);*/
		//createIndex(strTableName, "id");
		//BPlusTree<Integer> loadedIntegerBPlusTree = BPlusTree.loadFromFile("idbplustree.ser", 3, Integer.class);
		//System.out.println(loadedIntegerBPlusTree);
		
		//htblColNameValue.clear();
		//htblColNameValue.put("id", 14);
		//htblColNameValue.put("age", 45);
		//htblColNameValue.put("name", "ismail");
		//htblColNameValue.put("major", "Law");
		//htblColNameValue.put("gpa",  4.0);
		//htblColNameValue.put("phone", "666");
		//htblColNameValue.put("email", "ddez");
		//htblColNameValue.put("address", "dezdd");
		//dbApp.insertIntoTable(strTableName,htblColNameValue);
		//tempTable = Table.deserialize(strTableName);
		//tempTable.display();
		
		/*Hashtable<String,Object> htblColNameValue = new Hashtable<>( ); 
		htblColNameValue.put("id", 90);
		htblColNameValue.put("name", "Ismail");
		htblColNameValue.put("major", "MET");
		htblColNameValue.put("age", 12);
		htblColNameValue.put("gpa", 0.7);
		htblColNameValue.put("phone", "111");
		htblColNameValue.put("email", "aaa");
		htblColNameValue.put("address", "aaa");
		Table tempTable;
		tempTable = Table.deserialize(strTableName); */
		
		

		//createIndex(strTableName, "id");
		//createIndex(strTableName, "name");
		//createIndex(strTableName, "gpa");
		//BPlusTree<String> loadedStringBPlusTree = BPlusTree.loadFromFile("namebplustree.ser", 3, String.class);
		//System.out.println(loadedStringBPlusTree);
		//BPlusTree<Double> loadedDoubleBPlusTree = BPlusTree.loadFromFile("gpabplustree.ser", 3, Double.class);
		//System.out.println(loadedDoubleBPlusTree);
		//BPlusTree<Integer> loadedIntegerBPlusTree = BPlusTree.loadFromFile("idbplustree.ser", 3, Integer.class);
		//System.out.println(loadedIntegerBPlusTree);
		//Table tempTable = Table.deserialize(strTableName);
		//System.out.println(tempTable.Indexes.get(2));
		/*
		Hashtable <String,String> htblColNameType = new Hashtable <>( ); 
		htblColNameType.put("id", "java.lang.Integer"); 
		htblColNameType.put("age", "java.lang.Integer"); 
		htblColNameType.put("name", "java.lang.String"); 
		htblColNameType.put("major", "java.lang.String"); 
		htblColNameType.put("gpa", "java.lang.Double"); 
		htblColNameType.put("phone", "java.lang.String"); 
		htblColNameType.put("email", "java.lang.String"); 
		htblColNameType.put("address", "java.lang.String"); 
		
		try {
		dbApp.createTable( strTableName, "id", htblColNameType);
	} catch (DBAppException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		Hashtable<String,Object> htblColNameValue = new Hashtable<>( ); 
		
		Table tempTable = Table.deserialize(strTableName);

		htblColNameValue.put("id", 1);
		htblColNameValue.put("name", "Aml");
		htblColNameValue.put("major", "MET");
		htblColNameValue.put("age", 12);
		htblColNameValue.put("gpa", 0.7);
		htblColNameValue.put("phone", "111");
		htblColNameValue.put("email", "aaa");
		htblColNameValue.put("address", "aaa");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 2);
		htblColNameValue.put("age", 19);
		htblColNameValue.put("name", "Rowayda");
		htblColNameValue.put("major", "IET");
		htblColNameValue.put("gpa", 0.8);
		htblColNameValue.put("phone", "222");
		htblColNameValue.put("email", "bbb");
		htblColNameValue.put("address", "bbb");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 3);
		htblColNameValue.put("age", 30);
		htblColNameValue.put("name", "Rotana");
		htblColNameValue.put("major", "IET");
		htblColNameValue.put("gpa", 0.8);
		htblColNameValue.put("phone", "333");
		htblColNameValue.put("email", "ccc");
		htblColNameValue.put("address", "ccc");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 4);
		htblColNameValue.put("age", 25);
		htblColNameValue.put("name", "Farah");
		htblColNameValue.put("major", "DMET");
		htblColNameValue.put("gpa",  0.7);
		htblColNameValue.put("phone", "444");
		htblColNameValue.put("email", "ddd");
		htblColNameValue.put("address", "ddd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);	
		htblColNameValue.clear();
		htblColNameValue.put("id", 5);
		htblColNameValue.put("age", 35);
		htblColNameValue.put("name", "Rana");
		htblColNameValue.put("major", "DMET");
		htblColNameValue.put("gpa",  1.0);
		htblColNameValue.put("phone", "555");
		htblColNameValue.put("email", "dde");
		htblColNameValue.put("address", "dedd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", 6);
		htblColNameValue.put("age", 45);
		htblColNameValue.put("name", "Ahmed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "666");
		htblColNameValue.put("email", "ddez");
		htblColNameValue.put("address", "dezdd");
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		tempTable = Table.deserialize(strTableName);
		tempTable.display();
		*/
		//createIndex(strTableName, "id");
		/*
		SQLTerm[] arraySqlTerms= new SQLTerm[3];
		arraySqlTerms[0] = new SQLTerm(strTableName, "name", ">", "Aml");
		arraySqlTerms[1] = new SQLTerm(strTableName, "age", "<=", 30);
		arraySqlTerms[2] = new SQLTerm(strTableName, "gpa", "<", 1.0);
		
		String[] operators = {"AND" , "AND"};
		
		
		Iterator<Tuple> iter = dbApp.selectFromTable(arraySqlTerms, operators);
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		
		

		htblColNameValue.clear();
		htblColNameValue.put("name", "Ismail");
		dbApp.updateTable(strTableName, "6", htblColNameValue);
	    tempTable = Table.deserialize(strTableName);
		tempTable.display();*/
		
		//Hashtable<String,Object> htblColNameValue = new Hashtable<>( );
		//htblColNameValue.put("name", "Ismail");
		//dbApp.deleteFromTable(strTableName, htblColNameValue);
		//Table tempTable = Table.deserialize(strTableName);
		//tempTable.display();
	/*			
		
//		SQLTerm[] sqlterms = new SQLTerm[1];
//		
//		sqlterms[0] = new SQLTerm(strTableName, "name", "=", "Farah");
//		
//		Iterator<Tuple> iterator= dbApp.selectFromTable(sqlterms, null);
//		
		String[] columns = new String[3];
		columns[0] = "age";
		columns[1] = "name"; 
		columns[2] = "gpa";
		
		//createIndex(strTableName, columns);
		
		String IndexName1 = strTableName +"_"+ columns[0] + "_" + columns[1] + "_" + columns[2] ;
		
		
		columns = new String[3];
		columns[0] = "phone";
		columns[1] = "email"; 
		columns[2] = "address";
		
		//createIndex(strTableName, columns);
		
		String IndexName2 = strTableName +"_"+ columns[0] + "_" + columns[1] + "_" + columns[2] ;
		
		
		//inserting tuple after creating an indexes.. it should be inserted in both
		htblColNameValue.clear();
		htblColNameValue.put("id", 7);
		htblColNameValue.put("age", 55);
		htblColNameValue.put("name", "Mohamed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  2.0);
		htblColNameValue.put("phone", "777");
		htblColNameValue.put("email", "mmm");
		htblColNameValue.put("address", "mmm");
//		htblColNameValue.put("date of birth", new Date( "45/4/4"));
		dbApp.insertIntoTable(strTableName,htblColNameValue);

		
		htblColNameValue.clear();
		htblColNameValue.put("id", 8);
		htblColNameValue.put("age", 66);
		htblColNameValue.put("name", "Mohamed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "788");
		htblColNameValue.put("email", "mmmm");
		htblColNameValue.put("address", "mmim");
//		htblColNameValue.put("date of birth", new Date( "45/4/4"));
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		
		
		//duplicate
		htblColNameValue.clear();
		htblColNameValue.put("id", 20);
		htblColNameValue.put("age", 66);
		htblColNameValue.put("name", "Mohamed");
		htblColNameValue.put("major", "Law");
		htblColNameValue.put("gpa",  4.0);
		htblColNameValue.put("phone", "788");
		htblColNameValue.put("email", "mmmm");
		htblColNameValue.put("address", "mmim");
//		htblColNameValue.put("date of birth", new Date( "45/4/4"));
		dbApp.insertIntoTable(strTableName,htblColNameValue);
		
		
//		htblColNameValue.clear();
//		htblColNameValue.put("id", 20);
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
		
		SQLTerm[] arraySqlTerms= new SQLTerm[3];
		arraySqlTerms[0] = new SQLTerm(strTableName, "name", ">", "Aml");
		arraySqlTerms[1] = new SQLTerm(strTableName, "age", "<=", 30);
		arraySqlTerms[2] = new SQLTerm(strTableName, "gpa", "<", 1.0);
		
		String[] operators = {"AND" , "AND"};
		
		
		//Iterator<Tuple> iter = dbApp.selectFromTable(arraySqlTerms, operators);
		//while(iter.hasNext()) {
		//	System.out.println(iter.next());
		//}
		
//		displayTable(strTableName);
		//dbApp.displayIndex(strTableName, IndexName1);
		//dbApp.displayIndex(strTableName, IndexName2);
		
		
		
//		htblColNameValue.clear();
//		htblColNameValue.put("id", 5);
//		htblColNameValue.put("age", 15);
//		htblColNameValue.put("name", "Rana");
//		htblColNameValue.put("major", "DMET");
//		htblColNameValue.put("gpa",  0.7);
//		dbApp.insertIntoTable(strTableName,htblColNameValue);
//	
//		displayTable(strTableName);
//		dbApp.displayIndex(strTableName, IndexName);
//		
//		htblColNameValue.clear();
//		
//		htblColNameValue.put("age", 12);
//		htblColNameValue.put("name", "Aml");
//		
//		
////		htblColNameValue.put("id", 5);
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		
////		htblColNameValue.clear();
////		htblColNameValue.put("age", 30);
////
////		dbApp.updateTable(strTableName,"5" , htblColNameValue);
//		displayTable(strTableName);
//		dbApp.displayIndex(strTableName, IndexName);

		
		
		
		
		
		

*/
				
			
}
}
