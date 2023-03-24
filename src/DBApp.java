import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class DBApp {
	Vector<Table> tables;
	String MaximumRowsCountinTablePage;
	String MaximumEntriesinOctreeNode;
	public DBApp() throws IOException {
		tables = new Vector<Table>();
		File f =new File("resources/DBApp.config");
		FileInputStream fis = new FileInputStream(f);
		Properties p = new Properties();
		p.load(fis);
		 MaximumRowsCountinTablePage = p.getProperty("MaximumRowsCountinTablePage");
		 MaximumEntriesinOctreeNode = p.getProperty("MaximumEntriesinOctreeNode");
	}

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
			Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
			table.rows.add(page);
		}

		String pk = table.getPK();
		boolean test = false;

		//for the case that the insertion is the largest in the table
		int totalPages = table.rows.size();
		Hashtable<Integer,Integer > eachPageSizeBefore = new Hashtable<Integer, Integer>();
		for(int x = 0; x < table.rows.size(); x++) {
			eachPageSizeBefore.put(x, table.rows.get(x).tuples.size());
		}


		for(Page p : table.rows ) {
//		for(int j = 0;j<table.rows.size();j++ ) {
//			Page p = table.rows.get(j);
			int count = 0;
			if(p.tuples.isEmpty()) {
				p.tuples.insertElementAt(htblColNameValue, 0);
			}
			for(int i = 0;i < p.tuples.size();i++) {
				Hashtable<String,Object> tuple = p.tuples.get(i);
				if(tuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) > 0) {
					p.tuples.insertElementAt(htblColNameValue, count);
					redistributeIns(table);
					test = true;
					break;
				}
				count ++;
			}
			if(test) {break;}
//			for(Hashtable<String,Object> tuple : p.tuples) {
//
//				if(tuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) > 0) {
//					p.tuples.insertElementAt(htblColNameValue, count);
//					redistributeIns(table);
//				}
//				count ++;
//			}
		}

		Hashtable<Integer,Integer > eachPageSizeAfter = new Hashtable<Integer, Integer>();
		for(int x2 = 0;x2<table.rows.size();x2++) {
			eachPageSizeAfter.put(x2, table.rows.get(x2).tuples.size());
		}

		if(eachPageSizeBefore.equals(eachPageSizeAfter)) {
			if(table.rows.get(table.rows.size()-1).tuples.size() == Integer.parseInt(MaximumRowsCountinTablePage)) {
				Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
				page.tuples.add(htblColNameValue);
				table.rows.add(page);
			}
			else {
				table.rows.get(table.rows.size()-1).tuples.add(htblColNameValue);
			}
		}

	}

	public void redistributeIns(Table table) {
		for(int i=0;i<table.rows.size();i++)
		{
//			Vector<Page> v1 = table.rows;
//			Vector<Hashtable<String,Object>> v2= v1.get(i).tuples;
			if(table.rows.get(i).tuples.size()>Integer.parseInt(MaximumRowsCountinTablePage))
			{
				if(i==table.rows.size()-1)
				{
					Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
					table.rows.add(page);
					page.tuples.add(table.rows.get(i).tuples.get(table.rows.get(i).tuples.size()-1));
					table.rows.get(i).tuples.remove(table.rows.get(i).tuples.size()-1);
				}
				else
				{
					table.rows.get(i+1).tuples.insertElementAt(table.rows.get(i).tuples.get(table.rows.get(i).tuples.size()-1),0);
					table.rows.get(i).tuples.remove(table.rows.get(i).tuples.size()-1);
				}
			}
		}
	}


	public static void main(String[] args) throws IOException, DBAppException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp( );
		Hashtable htblColNameType = new Hashtable( );
		Hashtable htblColNameMin = new Hashtable( );
		Hashtable htblColNameMax = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		dbApp.createTable( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
		Hashtable htblColNameValue = new Hashtable( );
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		dbApp.insertIntoTable( strTableName , htblColNameValue );

		Hashtable htblColNameValue1 = new Hashtable( );
		htblColNameValue1.put("id", new Integer( 2343433 ));
		htblColNameValue1.put("name", new String("youssef" ) );
		htblColNameValue1.put("gpa", new Double( 0.95 ) );
		dbApp.insertIntoTable( strTableName , htblColNameValue1 );

		Hashtable htblColNameValue2 = new Hashtable( );
		htblColNameValue2.put("id", new Integer( 2343431 ));
		htblColNameValue2.put("name", new String("Ahmed" ) );
		htblColNameValue2.put("gpa", new Double( 0.95 ) );
		dbApp.insertIntoTable( strTableName , htblColNameValue2 );

		Hashtable htblColNameValue3 = new Hashtable( );
		htblColNameValue3.put("id", new Integer( 2343430 ));
		htblColNameValue3.put("name", new String("mohamed" ) );
		htblColNameValue3.put("gpa", new Double( 0.95 ) );
		dbApp.insertIntoTable( strTableName , htblColNameValue3 );

		Hashtable htblColNameValue5 = new Hashtable( );
		htblColNameValue5.put("id", new Integer( 2343429 ));
		htblColNameValue5.put("name", new String("zoz" ) );
		htblColNameValue5.put("gpa", new Double( 0.95 ) );
		dbApp.insertIntoTable( strTableName , htblColNameValue5 );
//
//		Hashtable htblColNameValue4 = new Hashtable( );
//		htblColNameValue4.put("id", new Integer( 2343433 ));
//		htblColNameValue4.put("name", new String("zoz" ) );
//		htblColNameValue4.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue4 );

		Table t = dbApp.tables.get(0);
		for(int i = 0;i < t.rows.size();i++) {
			Page p = t.rows.get(i);
			for(int j = 0;j<p.tuples.size();j++) {
				Hashtable<String,Object> h = p.tuples.get(j);
				System.out.println(h );
			}
			System.out.println();
		}
	}

//	File f =new File("resources/DBApp.config");
//	FileInputStream fis = new FileInputStream(f);
//	Properties p = new Properties();
//		p.load(fis);
//	String MaximumRowsCountinTablePage = p.getProperty("MaximumRowsCountinTablePage");
//		System.out.println(MaximumRowsCountinTablePage);

}
