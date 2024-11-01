package database.java;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.text.ParseException;
import java.util.Vector;

public class Table implements Serializable {
   
	private static final long serialVersionUID = 1L;	
	Hashtable<Integer,String> pages;
	Hashtable<Integer,String> Indexes;
	String TableName;
	int numberOfPages;
	int numberOfIndexes;
	
	public Table(String strTableName) {
		this.TableName = strTableName;
		pages = new Hashtable<>();
		Indexes = new Hashtable<>();
		numberOfPages = 0;
		numberOfIndexes = 0;
		
	}
	
	public boolean hasIndex() {
		return ! Indexes.isEmpty();
	}

	public static void serialize(Table table) throws DBAppException {
		try {			
			FileOutputStream fileOut = new FileOutputStream("./src/main/resources/data/" + table.TableName + ".bin");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(table);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException("IO Exception while writing to disk\t Table" + table.TableName);
		}
	}
	
	public static Table deserialize(String tableName) throws DBAppException {
		try {
			FileInputStream fileIn = new FileInputStream("./src/main/resources/data/"+tableName+".bin");
		
			
			ObjectInputStream in = new ObjectInputStream(fileIn);

			Table table = (Table) in.readObject();
			in.close();
			fileIn.close();

			return table;
		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException(
					"IO Exception | Probably wrong table name (tried to operate on a table that does not exist !");
		} catch (ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
		}
	}
	
	public String getPrimaryKey() {
		try {
			File metadataFile = new File("./src/main/resources/metadata.csv");
			FileReader fileReader = new FileReader(metadataFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			while(line != null)
			{
				String[] data = line.split(",");
				if(data[0].equals(this.TableName) && (data[3]).equals("true") ) {
					return data[1];
				}
				line = bufferedReader.readLine();
			}
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Hashtable<String, String> readTable() {
		try {
			File metadataFile = new File("./src/main/resources/metadata.csv");
			FileReader fileReader = new FileReader(metadataFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			Hashtable<String , String> ColNameType = new Hashtable<>();
			String line = bufferedReader.readLine();
			while(line != null)
			{
				String[] data = line.split(",");
				if(data[0].equals(this.TableName)) {
					ColNameType.put(data[1], data[2]);
				}
				line = bufferedReader.readLine();
			}
			
			return ColNameType;
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

	public String getPrimaryKeyType() {
		Hashtable<String, String> ColNameType = readTable();
		String primaryKey = getPrimaryKey();
		return ColNameType.get(primaryKey);
		
	}
	
	public Hashtable<String, String> getMin(){
		try {
			File metadataFile = new File("./src/main/resources/metadata.csv");
			FileReader fileReader = new FileReader(metadataFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);			
			Hashtable<String , String> ColNameMin = new Hashtable<>();
			String line = bufferedReader.readLine();
			while(line != null)
			{
				String[] data = line.split(",");
				if(data[0].equals(this.TableName)) {
					ColNameMin.put(data[1], data[6]);
				}
				line = bufferedReader.readLine();

			}
	
			return ColNameMin;
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	public Hashtable<String, String> getMax(){
		try {
			File metadataFile = new File("./src/main/resources/metadata.csv");
			FileReader fileReader = new FileReader(metadataFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			
			Hashtable<String , String> ColNameMax = new Hashtable<>();
			String line = bufferedReader.readLine();
			while(line != null)
			{
				String[] data = line.split(",");
				
				if(data[0].equals(this.TableName)) {
					ColNameMax.put(data[1], data[7]);
				}
				line = bufferedReader.readLine();
			}
			
			return ColNameMax;
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	public static Object parse(String value, String type) { 

		switch (type) {
		case "java.lang.Integer":
		{
			return Integer.parseInt(value);
		}
		case "java.lang.Double":
		{
			return Double.parseDouble(value);
		}
		case "java.lang.String":
		{
			return value;
		}
		default:
			return null;
		}
	}
		
	private boolean check(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Hashtable<String, String> ColNameType = readTable();
		for(String key : htblColNameValue.keySet()) {
			if(!ColNameType.containsKey(key)) {
				throw new DBAppException("wrong column name");
			}
			String inputType = htblColNameValue.get(key).getClass().getName();
			String originalType = ColNameType.get(key);
			
			if(! inputType.equals(originalType)) {
				throw new DBAppException("values do not match types");
			}
			
			Object value = htblColNameValue.get(key);
			String type = ColNameType.get(key);
		}
		return true;
	}
	
	public void update(String strClusteringKeyValue, Hashtable<String, Object> newValue) throws DBAppException {
		check(newValue); 
			
		if(strClusteringKeyValue == null) {
			throw new DBAppException("Primary Key field is null");
		}
		String pKtype = getPrimaryKeyType();
		Object pk = parse(strClusteringKeyValue, pKtype);
		/*if(hasIndex()) {
			String indexName = Indexes.get(0);
			for(Integer key: pages.keySet()) {
				Page page = Page.deserialize(pages.get(key));
				if(inRange(pk,page.minPK, page.maxPK)) {
					
					page.updateTupleWithIndex(pk, newValue , pKtype, indexName);
					Page.serialize(page);
					return;
				}
			}			
		}*/
		for(Integer key: pages.keySet()) {
			Page page = Page.deserialize(pages.get(key));
			if(inRange(pk,page.minPK, page.maxPK)) {
				page.updateTuple(pk, newValue , pKtype);
				Page.serialize(page);
				return;
			}
		}
		
		}	
	
	public void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
	    check(htblColNameValue);
	    boolean flag=false;
	    Iterator<Integer> iter = pages.keySet().iterator();
	    while (iter.hasNext()) {
	        Integer key = iter.next();
	        Page page = Page.deserialize(pages.get(key));
	        String primaryKey = getPrimaryKey();
	        if(htblColNameValue.containsKey(primaryKey)) {
	        	String pKtype = getPrimaryKeyType();
				Object pk = htblColNameValue.get(primaryKey);
				if(inRange(pk,page.minPK , page.maxPK)) {
						page.delete2(htblColNameValue, pk, pKtype, Indexes);
						Page.serialize(page);
						return;
				}
	        }
	      boolean s = page.delete(htblColNameValue, Indexes);
	       flag=flag||s;	       
	        if (page.isEmpty()) {
	            FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + page.pageName + ".ser");
	            fileIn.close();
	            File file = new File("./src/main/resources/data/" + page.pageName + ".ser");
	            file.delete();
	            iter.remove(); 
	        } else {
	            Page.serialize(page);
	        }
	    }
	    if(!flag)
	    {
	    	throw new DBAppException("Tuples to be deleted does not exist");
	    }
	    
	}
	
	public boolean inRange(Object pk, Object min, Object max) { 
		String pKtype = getPrimaryKeyType();
		switch (pKtype) {
		case "java.lang.Integer":{
			if (((Integer) pk).compareTo((Integer)min) >= 0 && ((Integer) pk).compareTo((Integer)max) <= 0) {
				
				return true;
			}
			break;
		}
		case "java.lang.Double":{
			if (((Double) pk).compareTo((Double)min) >= 0 && ((Double) pk).compareTo((Double)max) <= 0) {
				return true;
			}
			break;
		}
			
		case "java.lang.String":{
			if (((String) pk).compareTo((String)min) >= 0 && ((String) pk).compareTo((String)max) <= 0) {
				return true;
			}
			break;
		}
			
		default:
			break;
		}
		return false;
	}
	
	public void insertInIndex(Tuple tuple, String pageName,String indexName, String indexType) throws DBAppException {
		if(indexType.equals("java.lang.String")){
			BPlusTree<String> loadedStringBPlusTree = BPlusTree.loadFromFile(indexName+"bplustree.ser", 3, String.class);
			String tempString = (String) tuple.ColNameValue.get(indexName);
			loadedStringBPlusTree.insert(tempString, pageName);
			loadedStringBPlusTree.saveToFile(indexName+"bplustree.ser");
        }
        else if(indexType.equals("java.lang.Integer")){
			BPlusTree<Integer> loadedIntegerBPlusTree = BPlusTree.loadFromFile(indexName+"bplustree.ser", 3, Integer.class);
			Integer tempInteger = (Integer) tuple.ColNameValue.get(indexName);
			loadedIntegerBPlusTree.insert(tempInteger, pageName);
			loadedIntegerBPlusTree.saveToFile(indexName+"bplustree.ser");
        }
        else if(indexType.equals("java.lang.Double")){
			BPlusTree<Double> loadedDoubleBPlusTree = BPlusTree.loadFromFile(indexName+"bplustree.ser", 3, Double.class);
			Double tempDouble = (Double) tuple.ColNameValue.get(indexName);
			loadedDoubleBPlusTree.insert(tempDouble, pageName);
			loadedDoubleBPlusTree.saveToFile(indexName+"bplustree.ser");
        }
	}
	
	public boolean insert(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		check(htblColNameValue);		
		String primaryKey = getPrimaryKey();
		String pKtype = getPrimaryKeyType();
		Object pk = htblColNameValue.get(primaryKey);
		if(pk==null) {
			throw new DBAppException("primary key does not exist");
		}
		Tuple newTuple = new Tuple(htblColNameValue, primaryKey ,pk ,pKtype);
		
		//case 0: empty table
		if(pages.isEmpty()) { //create a new page
			String firstPageName = TableName + "_"+ numberOfPages;
			Page firstPage = new Page(firstPageName,pk, pk);
			pages.put(numberOfPages, firstPageName);
			numberOfPages++;
			firstPage.insert(newTuple , 0);
			Page.serialize(firstPage);
			
			/*if(hasIndex()) {
				for(Integer key: Indexes.keySet()) {
					String indexName = Indexes.get(key);
					String indexType;
					if(htblColNameValue.get(indexName) instanceof String){
						 indexType = "java.lang.String";
					}
					if(htblColNameValue.get(indexName) instanceof Integer){
						 indexType = "java.lang.String";
					}
					if(htblColNameValue.get(indexName) instanceof Double){
						 indexType = "java.lang.String";
					}
					else{
						throw new DBAppException("indexType is not an acceptable type");
					}
					
					insertInIndex(newTuple, firstPageName, indexName,indexType);
					
				}
			}*/
			return true;
		}
		
		//find the right page
		int size = pages.size();
		int pageIndex = 0;
		String pkType = getPrimaryKeyType();
		if(Indexes.contains(primaryKey)){
			if(pkType.equals("java.lang.String")){
				BPlusTree<String> loadedStringBPlusTree = BPlusTree.loadFromFile(primaryKey+"bplustree.ser", 3, String.class);
				String loadedStringBPlusTreePrint = loadedStringBPlusTree.toString();
				String place = loadedStringBPlusTree.findClosestValue((String) pk, loadedStringBPlusTreePrint);
				if(place.equals("First")){
					pageIndex = 0;
				}
				else if(place.equals("Last")){
					pageIndex = size - 1;
				}
				else{
					pageIndex = BPlusTree.extractPageNumber(place);
				}
			}
			else if(pkType.equals("java.lang.Integer")){
				BPlusTree<Integer> loadedIntegerBPlusTree = BPlusTree.loadFromFile(primaryKey+"bplustree.ser", 3, Integer.class);
				String loadedIntegerBPlusTreePrint = loadedIntegerBPlusTree.toString();
				String place = loadedIntegerBPlusTree.findClosestValue((Integer) pk, loadedIntegerBPlusTreePrint);
				if(place.equals("First")){
					pageIndex = 0;
				}
				else if(place.equals("Last")){
					pageIndex = size - 1;
				}
				else{
					pageIndex = BPlusTree.extractPageNumber(place);
				}
			}
			else if(pkType.equals("java.lang.Double")){
				BPlusTree<Double> loadedDoubleBPlusTree = BPlusTree.loadFromFile(primaryKey+"bplustree.ser", 3, Double.class);
				String loadedDoubleBPlusTreePrint = loadedDoubleBPlusTree.toString();
				String place = loadedDoubleBPlusTree.findClosestValue((Double) pk, loadedDoubleBPlusTreePrint);
				if(place.equals("First")){
					pageIndex = 0;
				}
				else if(place.equals("Last")){
					pageIndex = size - 1;
				}
				else{
					pageIndex = BPlusTree.extractPageNumber(place);
				}
			}
		}
		else{
		for(int i = 0; i< size ; i++) {
			String pageName = pages.get(i);
			Page page = Page.deserialize(pageName);
			if(inRange(pk, page.minPK, page.maxPK)) {
				pageIndex = i;
				Page.serialize(page);
				break;
			}

			//special case
			String nextPageName = pages.get(i+1);
			if(nextPageName!=null) {
				Page nextPage = Page.deserialize(nextPageName);
				if(inRange(pk, page.maxPK, nextPage.minPK) && (!page.isFull())) {
					page.insert(newTuple, page.size());
					Page.serialize(nextPage);
					Page.serialize(page);
					if(Indexes.contains(primaryKey)) {
							String indexName = Indexes.get(primaryKey);
							String indexType;
							if(htblColNameValue.get(indexName) instanceof String){
								 indexType = "java.lang.String";
							}
							else if(htblColNameValue.get(indexName) instanceof Integer){
								 indexType = "java.lang.Integer";
							}
							else if(htblColNameValue.get(indexName) instanceof Double){
								 indexType = "java.lang.Double";
							}
							else{
								throw new DBAppException("indexType is not an acceptable type");
							}
							
							insertInIndex(newTuple, nextPageName, indexName,indexType);
							
						
					}
					return true;
				}
			}
			if((!page.isFull())&&(nextPageName==null)){
				pageIndex = i;
				Page.serialize(page);
				break;
			}
			if(pageIndex ==0  && (((Comparable)pk).compareTo((Comparable)page.minPK)<0)) {
				
				pageIndex = i;
				Page.serialize(page);
				break;
			}
			if(nextPageName == null) { // tuples exceeded range and next page not created 
				// create new page
				String newPageName = TableName +"_" + numberOfPages;
				Page newPage = new Page(newPageName, pk, pk);
				pages.put(numberOfPages, newPageName);
				numberOfPages++;
				newPage.insert(newTuple , 0);
				Page.serialize(newPage);
				Page.serialize(page);
				
				if(Indexes.contains(primaryKey)) {
						String indexName = Indexes.get(primaryKey);
						String indexType;
						if(htblColNameValue.get(indexName) instanceof String){
							 indexType = "java.lang.String";
						}
						else if(htblColNameValue.get(indexName) instanceof Integer){
							 indexType = "java.lang.Integer";
						}
						else if(htblColNameValue.get(indexName) instanceof Double){
							 indexType = "java.lang.Double";
						}
						else{
							throw new DBAppException("indexType is not an acceptable type");
						}
						
						insertInIndex(newTuple, nextPageName, indexName,indexType);
						
					
				}
				
				return true;
				
				
			}
			
			Page.serialize(page);
		}
		}
		
		String rightPageName = pages.get(pageIndex);
		Page rightPage = Page.deserialize(rightPageName); 
		int tupleIndex = rightPage.binarysearch(pk, pKtype);
		
		
		
		//case 1: page is not full
		if(!rightPage.isFull()) {
			//case 1-1: tuple place is empty 
			if(rightPage.getTuple(tupleIndex) == null) {
				rightPage.insert(newTuple, tupleIndex);
				Page.serialize(rightPage);
				if(Indexes.contains(primaryKey)) {
							String indexName = Indexes.get(primaryKey);
							String indexType;
							System.out.println(htblColNameValue.get(indexName)+"   "+"ssssssssssss");
							System.out.println(htblColNameValue.get(indexName) instanceof String);
							if(htblColNameValue.get(indexName) instanceof String){
								 indexType = "java.lang.String";
							}
							else if(htblColNameValue.get(indexName) instanceof Integer){
								 indexType = "java.lang.Integer";
							}
							else if(htblColNameValue.get(indexName) instanceof Double){
								 indexType = "java.lang.Double";
							}
							else{
								throw new DBAppException("indexType is not an acceptable type");
							}
							
							insertInIndex(newTuple, rightPageName, indexName,indexType);
							
						
					
				}
				return true;
			}
			//case 1-2: tuple place is not empty(must shift)
			if(rightPage.getTuple(tupleIndex) != null) {
				//if pk already exists
				if(rightPage.getTuple(tupleIndex).primaryKey.equals(pk)) {
					throw new DBAppException("primary key already exists");
				}
				rightPage.shift(tupleIndex);
				rightPage.insert(newTuple, tupleIndex);
				Page.serialize(rightPage);
				if(Indexes.contains(primaryKey)) {
						String indexName = Indexes.get(primaryKey);
						String indexType;
						if(htblColNameValue.get(indexName) instanceof String){
							 indexType = "java.lang.String";
						}
						else if(htblColNameValue.get(indexName) instanceof Integer){
							 indexType = "java.lang.String";
						}
						else if(htblColNameValue.get(indexName) instanceof Double){
							 indexType = "java.lang.String";
						}
						else{
							throw new DBAppException("indexType is not an acceptable type");
						}
						
						insertInIndex(newTuple, rightPageName, indexName,indexType);
						
					
				
				}
				return true;
			}
		}
		
		//case 2: page is full
		if(rightPage.isFull()) {
			//case 2-1: tuple place is empty 
			if(rightPage.getTuple(tupleIndex) == null) {
				rightPage.insert(newTuple, tupleIndex);
				Page.serialize(rightPage);
				if(Indexes.contains(primaryKey)) {
							String indexName = Indexes.get(primaryKey);
							String indexType;
							if(htblColNameValue.get(indexName) instanceof String){
								 indexType = "java.lang.String";
							}
							else if(htblColNameValue.get(indexName) instanceof Integer){
								 indexType = "java.lang.Integer";
							}
							else if(htblColNameValue.get(indexName) instanceof Double){
								 indexType = "java.lang.Double";
							}
							else{
								throw new DBAppException("indexType is not an acceptable type");
							}
							
							insertInIndex(newTuple, rightPageName, indexName,indexType);
							
						
					
					
				}
				return true;
			}
			//case 2-2: tuple place is not empty(must shift)
			if(rightPage.getTuple(tupleIndex) != null) {
				//if pk already exists
				if(rightPage.getTuple(tupleIndex).primaryKey.equals(pk)) {
					throw new DBAppException("primary key already exists");
				}
				Tuple lastTuple = rightPage.getTuple(rightPage.size()-1);
				rightPage.shift(tupleIndex);
				rightPage.insert(newTuple, tupleIndex);
				Page.serialize(rightPage);
				if(Indexes.contains(primaryKey)) {

							String indexName = Indexes.get(primaryKey);
							String indexType;
							if(htblColNameValue.get(indexName) instanceof String){
								 indexType = "java.lang.String";
							}
							else if(htblColNameValue.get(indexName) instanceof Integer){
								 indexType = "java.lang.Integer";
							}
							else if(htblColNameValue.get(indexName) instanceof Double){
								 indexType = "java.lang.Double";
							}
							else{
								throw new DBAppException("indexType is not an acceptable type");
							}
							
							insertInIndex(newTuple, rightPageName, indexName,indexType);
							
						
					
					
				}
				
				//insert the last tuple (resulting from shifting) in the right page
				for(int i = pageIndex + 1 ; i<=size ;i++) {
					String nextPageName = pages.get(i);
					
					//case 2-2-1: no next page 
					if(nextPageName == null) {
						String pageName = TableName +"_"+ numberOfPages;
						Page page = new Page(pageName ,lastTuple.primaryKey, lastTuple.primaryKey);
						pages.put(numberOfPages, pageName);
						numberOfPages++;
						page.insert(lastTuple, 0);
						Page.serialize(page);
						if(Indexes.contains(primaryKey)) {

								String indexName = Indexes.get(primaryKey);
								String indexType;
								if(htblColNameValue.get(indexName) instanceof String){
									 indexType = "java.lang.String";
								}
								else if(htblColNameValue.get(indexName) instanceof Integer){
									 indexType = "java.lang.Integer";
								}
								else if(htblColNameValue.get(indexName) instanceof Double){
									 indexType = "java.lang.Double";
								}
								else{
									throw new DBAppException("indexType is not an acceptable type");
								}
								
								insertInIndex(newTuple, rightPageName, indexName,indexType);
								
							
						
							}
						return true;
					}
					Page nextPage = Page.deserialize(nextPageName);
					//case 2-2-2: next page is not full
					if(!nextPage.isFull()) {
						nextPage.shift(0);
						nextPage.insert(lastTuple, 0);
						Page.serialize(nextPage);
						if(Indexes.contains(primaryKey)) {

								String indexName = Indexes.get(primaryKey);
								String indexType;
								if(htblColNameValue.get(indexName) instanceof String){
									 indexType = "java.lang.String";
								}
								else if(htblColNameValue.get(indexName) instanceof Integer){
									 indexType = "java.lang.Integer";
								}
								else if(htblColNameValue.get(indexName) instanceof Double){
									 indexType = "java.lang.Double";
								}
								else{
									throw new DBAppException("indexType is not an acceptable type");
								}
								
								insertInIndex(newTuple, rightPageName, indexName,indexType);
								
							
						
						}
						return true;
					}
					//case 2-2-3: next page is full
					if(nextPage.isFull()) {
						newTuple = lastTuple;
						lastTuple = nextPage.getTuple(nextPage.size()-1);
						nextPage.shift(0);
						nextPage.insert(newTuple, 0);
						Page.serialize(nextPage);
						if(Indexes.contains(primaryKey)) {
								String indexName = Indexes.get(primaryKey);
								String indexType;
								if(htblColNameValue.get(indexName) instanceof String){
									 indexType = "java.lang.String";
								}
								else if(htblColNameValue.get(indexName) instanceof Integer){
									 indexType = "java.lang.Integer";
								}
								else if(htblColNameValue.get(indexName) instanceof Double){
									 indexType = "java.lang.Double";
								}
								else{
									throw new DBAppException("indexType is not an acceptable type");
								}
								
								insertInIndex(newTuple, rightPageName, indexName,indexType);
								
							
						
						}
					}
				}
			}
		}
		return false;
	}
	 
    public void display() throws DBAppException {
		 for(Integer key: pages.keySet()) {
				Page page = Page.deserialize(pages.get(key));
				System.out.println("page "+key +":" + page.toString());
				Page.serialize(page);
				}
				
	 }
	 
	public Vector<Tuple> selectFromTable(SQLTerm s) throws DBAppException {
		 String colName = s._strColumnName;
		 String operator = s._strOperator;
		 Object value = s._objValue;
		 Vector<Tuple> result = new Vector<>();
		
		 switch(operator) {
		 case "=":
			 for(Integer key: pages.keySet()) {
				 Page page = Page.deserialize(pages.get(key));
				 for(Tuple t : page.tuples) {
					 Object valueInTuple = t.ColNameValue.get(colName);
					 if(valueInTuple.equals(value)) {
						 result.add(t);
					 }
					 
				 }
				 Page.serialize(page);
			 }
			 
			 break;
		 case "!=":
			 for(Integer key: pages.keySet()) {
				 Page page = Page.deserialize(pages.get(key));
				 for(Tuple t : page.tuples) {
					 Object valueInTuple = t.ColNameValue.get(colName);
					 if(!valueInTuple.equals(value)) {
						 result.add(t);
					 }
					 
				 }
				 Page.serialize(page);
			 }
			 break;
		 case ">":
			 for(Integer key: pages.keySet()) {
				 Page page = Page.deserialize(pages.get(key));
				 for(Tuple t : page.tuples) {
					 Object valueInTuple = t.ColNameValue.get(colName);
					 String type = valueInTuple.getClass().getName();
					 switch(type) {
					 case "java.lang.Integer":
						 if(((Integer)valueInTuple).compareTo((Integer)value)>0) {
							 result.add(t);
							
						 }
						 break;
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
				 Page.serialize(page);
			 }
			 
			 break;
		 case "<":
			 for(Integer key: pages.keySet()) {
				 Page page = Page.deserialize(pages.get(key));
				 for(Tuple t : page.tuples) {
					 Object valueInTuple = t.ColNameValue.get(colName);
					 String type = valueInTuple.getClass().getName();
					 switch(type) {
					 case "java.lang.Integer":
						 if(((Integer)valueInTuple).compareTo((Integer)value)<0) {
							 result.add(t);
							 
						 } break;
						 
					 case"java.lang.Double":
						 if(((Double)valueInTuple).compareTo((Double)value)<0) {
							 result.add(t);
							
						 } break;
						 
					 case "java.lang.String":
						 if(((String)valueInTuple).compareTo((String)value)<0) {
							 result.add(t);
							
						 } break;
					 default: break;
					 } 
				 }
				 Page.serialize(page);
			 }
			 break;
		 case ">=":
			 for(Integer key: pages.keySet()) {
				 Page page = Page.deserialize(pages.get(key));
				 for(Tuple t : page.tuples) {
					 Object valueInTuple = t.ColNameValue.get(colName);
					 String type = valueInTuple.getClass().getName();
					 switch(type) {
					 case "java.lang.Integer":
						 if(((Integer)valueInTuple).compareTo((Integer)value)>=0) {
							 result.add(t);
							 
						 } break;
						 
					 case"java.lang.Double":
						 if(((Double)valueInTuple).compareTo((Double)value)>=0) {
							 result.add(t);
							 
						 } break;
						 
					 case "java.lang.String":
						 if(((String)valueInTuple).compareTo((String)value)>=0) {
							 result.add(t);
							
						 } break;
					default: break;	 
					 } 
				 }
				 Page.serialize(page);
			 }
			 break;
		 case "<=":
			 for(Integer key: pages.keySet()) {
				 Page page = Page.deserialize(pages.get(key));
				 for(Tuple t : page.tuples) {
					 Object valueInTuple = t.ColNameValue.get(colName);
					 String type = valueInTuple.getClass().getName();
					 switch(type) {
					 case "java.lang.Integer":
						 if(((Integer)valueInTuple).compareTo((Integer)value)<=0) {
							 result.add(t);
							 
						 }
						 break;
					 case"java.lang.Double":
						 if(((Double)valueInTuple).compareTo((Double)value)<=0) {
							 result.add(t);
							
						 } break;
						 
					 case "java.lang.String":
						 if(((String)valueInTuple).compareTo((String)value)<=0) {
							 result.add(t);
							
						 } break;
					default: break;	 
					 } 
				 }
				 Page.serialize(page);
			 }
			 break;
		 default: break;
		 
	 }
		 return result;
		
		
	 }
	
	public void createIndex(String strTableName, String strColName) throws DBAppException {
	    Hashtable<String, String> ColNameType = readTable();
	    Table tempTable = deserialize(strTableName);
	    
	    if (tempTable.pages.isEmpty()) {
	        throw new DBAppException("Table " + strTableName + " is empty so an index is impossible.");
	    } else if (tempTable.Indexes.contains(strColName)) {
	        throw new DBAppException("Index " + strTableName + " already exists.");
	    } else if (!tempTable.readTable().containsKey(strColName)) {
	        throw new DBAppException("Table " + strTableName + " doesn't have the column " + strColName);
	    }

	    String colType = ColNameType.get(strColName);
	    switch (colType) {
	        case "java.lang.String":
	        	BPlusTree<String> bPlusTree = new BPlusTree<>(3, String.class);
	    	    for (int i = 0; i < tempTable.numberOfPages; i++) {
	    	        Page tempPage = Page.deserialize(strTableName + "_" + i);
	    	        for (int j = 0; j < tempPage.size(); j++) {
	    	            Tuple tempTuple = tempPage.tuples.get(j);
	    	            Object tempValue = tempTuple.ColNameValue.get(strColName);
	    	            bPlusTree.insert((String) tempValue, tempPage.pageName);
	    	        }
	    	    }
	    	    bPlusTree.saveToFile(strColName + "bplustree.ser");
	            break;
	        case "java.lang.Integer":
	        	BPlusTree<Integer> bPlusTree1 = new BPlusTree<>(3, Integer.class);
	    	    for (int i = 0; i < tempTable.numberOfPages; i++) {
	    	        Page tempPage = Page.deserialize(strTableName + "_" + i);
	    	        for (int j = 0; j < tempPage.size(); j++) {
	    	            Tuple tempTuple = tempPage.tuples.get(j);
	    	            Object tempValue = tempTuple.ColNameValue.get(strColName);
	    	            bPlusTree1.insert((Integer) tempValue, tempPage.pageName);
	    	        }
	    	    }
	    	    bPlusTree1.saveToFile(strColName + "bplustree.ser");
	            break;
	        case "java.lang.Double":
	        	BPlusTree<Double> bPlusTree11 = new BPlusTree<>(3, Double.class);
	    	    for (int i = 0; i < tempTable.numberOfPages; i++) {
	    	        Page tempPage = Page.deserialize(strTableName + "_" + i);
	    	        for (int j = 0; j < tempPage.size(); j++) {
	    	            Tuple tempTuple = tempPage.tuples.get(j);
	    	            Object tempValue = tempTuple.ColNameValue.get(strColName);
	    	            bPlusTree11.insert((Double) tempValue, tempPage.pageName);
	    	        }
	    	    }
	    	    bPlusTree11.saveToFile(strColName + "bplustree.ser");
	            break;
	        default:
	            throw new DBAppException("Unsupported data type for indexing: " + colType);
	    }
		tempTable.Indexes.put(tempTable.numberOfIndexes,strColName); 
		this.Indexes = tempTable.Indexes;
		this.numberOfIndexes++;
	    serialize(tempTable);
	}	
}
