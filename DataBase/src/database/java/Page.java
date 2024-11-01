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
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Vector;


import java.util.Properties;

public class Page  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7064945789775916652L;
	Object minPK;
	Object maxPK;
	int maxSize;
	Vector<Tuple> tuples;
	String pageName;
		
	public Page(String pageName , Object minPK, Object maxPK ) {
		this.maxSize = setMaxPageSize();
		this.minPK = minPK;
		this.maxPK = maxPK;
		this.tuples = new Vector<>(maxSize);
		this.pageName = pageName;
	}
	
	public static void serialize(Page page) throws DBAppException {
		try {
			FileOutputStream fileOut = new FileOutputStream("./src/main/resources/data/" + page.pageName + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException("IO Exception while writing to disk\t Page" + page.pageName);
		}
	}
	
	public static Page deserialize(String pageName) throws DBAppException {
		try {
			FileInputStream fileIn = new FileInputStream("./src/main/resources/data/" + pageName + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page page = (Page) in.readObject();
			in.close();
			fileIn.close();
			return page;
		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException(
					"IO Exception | Probably wrong page name (tried to operate on a page that does not exist !");
		} catch (ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
		}
	}

	public int setMaxPageSize() {
		 Properties props = new Properties();
		 try (FileInputStream fis = new FileInputStream("./src/main/resources/DBApp.config")) {
	            props.load(fis);
	            String maxsize = props.getProperty("MaximumRowsCountinTablePage");
	            return Integer.parseInt(maxsize);

	        } catch (IOException e) {
	            e.printStackTrace();
	        }
		  return 0;		
	}

	public boolean isEmpty() {
		return tuples.isEmpty();
	}
	
	public boolean isFull() {
		return tuples.size() >= maxSize;
	}
	
	public Tuple getTuple(int index) {
		if(index<0 || index>=tuples.size()) {
			return null;
		}
		return tuples.get(index);
	}
	
	public int size() {
		return tuples.size();
	}
	
	public void insert(Tuple newTuple , int index) {
		if(index==size()) {
			tuples.add(index, newTuple);
			updateMinMax();
		}
		else {
		tuples.set(index, newTuple);
		updateMinMax();
		}
		
	}
	
	public void updateTuple(Object pk, Hashtable<String, Object> newValue, String type) throws DBAppException {
		int index = binarysearch(pk, type);
		Tuple tuple = tuples.get(index);
		for (String columnName : newValue.keySet()) {
			  if (tuple.ColNameValue.containsKey(columnName)) {
				 if(columnName.equals(tuple.pkName)) {
					 throw new DBAppException("cannot update the primary key");
				 }
				 else {
					 tuple.ColNameValue.put(columnName, newValue.get(columnName));
				 }
			  }
			  else {
				  throw new DBAppException("wrong column name");
			  }
			}
	}
	
	public void shift(int index) {
		int last = tuples.size() -1;
		if(tuples.size()<maxSize) {
			tuples.add(tuples.get(last));
		}
		for(int i = last; i > index; i--) {
			tuples.set(i, tuples.get(i-1));
		}
	}
	
	public int binarysearch(Object pk, String type) {
		 int start = 0; 
		 int end = tuples.size()-1;
		
		 while(start <= end) {
			 int mid = start + (end - start)/2;
			 Object midPK = (tuples.get(mid)).primaryKey;
			 if(midPK.equals(pk)) {
				return mid;
			 }
			 else {
					switch (type) {
					case "java.lang.Integer":{
						if (((Integer) pk).compareTo((Integer)midPK) > 0 ) {
							start = mid+1;
						}
						else if (((Integer) pk).compareTo((Integer)midPK) < 0 ){
							end = mid -1;
						}
						break;
					}
					case "java.lang.Double":{
						if (((Double) pk).compareTo((Double)midPK) > 0) {
							start = mid+1;
						}
						else if (((Double) pk).compareTo((Double)midPK) < 0 ){
							end = mid -1;
						}
						break;
					}						
					case "java.lang.String":{
						if (((String) pk).compareTo((String)midPK) > 0) {
							start = mid+1;
						}
						else if (((String) pk).compareTo((String)midPK) < 0 ){
							end = mid -1;
						}
						break;
					}
						default: break;
					
			 }
			 }
			 
			
		 }
		 return start;
	}
	
	public boolean delete2(Hashtable<String, Object> htblColNameValue,Object pk, String type, Hashtable<Integer, String> indexes) throws DBAppException {
		int i = binarysearch(pk, type);
		
		if(i >= tuples.size()) {
			throw new DBAppException("tuple does not exist");
		}
		System.out.println(tuples.get(i).primaryKey);
		System.out.println(pk);
		if(!tuples.get(i).primaryKey.equals(pk)) {
			throw new DBAppException("tuple does not exist");	
}
		Tuple tuple = tuples.get(i);
		tuples.remove(i);
		return true;
		
	}

	public boolean delete(Hashtable<String, Object> htblColNameValue, Hashtable<Integer, String> indexes ) throws DBAppException  {
       boolean flag2 = false;
		for(int i = 0; i< size(); i++) {
			Tuple tuple = tuples.get(i);
			boolean flag = true;
			for(String key: htblColNameValue.keySet()) {
				if(!tuple.ColNameValue.containsKey(key)) {
					throw new DBAppException("wrong column name");
				}
				flag = flag && (tuple.ColNameValue.get(key)).equals(htblColNameValue.get(key));
			}
			if(flag) {		
				tuples.remove(i);
				flag2=true;
				i--;
			}
		}
		updateMinMax();
		return flag2;
	}
	
	public void updateMinMax() {
		if(size() == 0) {
			minPK = null;
			maxPK = null;
		}
		else{
		minPK = tuples.get(0).primaryKey;
		maxPK = tuples.get(size()-1).primaryKey;
		}
	}
	
	public String toString()
	{
		return tuples.toString();
	}

}

