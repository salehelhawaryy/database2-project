import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class DBApp {
	Vector<Table> tables;
	
	public void createTable(String tableName,String clusteringKey, Hashtable<String,String> ColNameType,  
		Hashtable<String, String> ColNameMin,Hashtable<String,String> ColNameMax) throws DBAppException
	{
		Table table =new Table(tableName,clusteringKey,ColNameType,ColNameMin,ColNameMax);
		tables.add(table);
	}

	public void insertIntoTable(String strTableName,
								Hashtable<String,Object> htblColNameValue)
			throws DBAppException
	{
		Table table = null;
		Boolean found = false;
		for(int i = 0; i< tables.size(); i++) {

			if(tables.get(i).getName().equals(strTableName)) {
				found = true;
				table = tables.get(i);
				break;
			}
		}
		if(!found) {
			throw new DBAppException();
		}

		if(table.rows.isEmpty()) {
			Page page = new Page(3);
			table.rows.add(page);
		}

		String pk = table.getPK();
		htblColNameValue.get(pk);



		for(Page p : table.rows ) {
			int count = 0;
			for(Hashtable<String,Object> tuple : p.tuples) {
				if(tuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) > 0) {
					p.tuples.insertElementAt(htblColNameValue, count);
				}
				count ++;
			}
		}

	}



	public static void main(String[] args) throws IOException {
//		System.out.println("test");
		String a = "1";
		String b = "2";

		System.out.println(b.compareTo(a));

			File f =new File("resources/DBApp.config");
	FileInputStream fis = new FileInputStream(f);
	Properties p = new Properties();
		p.load(fis);
	String MaximumRowsCountinTablePage = p.getProperty("MaximumRowsCountinTablePage");
	String MaximumEntriesinOctreeNode = p.getProperty("MaximumEntriesinOctreeNode");
		System.out.println(MaximumRowsCountinTablePage);
	}

//	File f =new File("resources/DBApp.config");
//	FileInputStream fis = new FileInputStream(f);
//	Properties p = new Properties();
//		p.load(fis);
//	String MaximumRowsCountinTablePage = p.getProperty("MaximumRowsCountinTablePage");
//		System.out.println(MaximumRowsCountinTablePage);

}
