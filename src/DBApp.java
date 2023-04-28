import java.io.*;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class DBApp {
	Vector<Table> tables;

	Hashtable<String, File> files;
	String MaximumRowsCountinTablePage;
	String MaximumEntriesinOctreeNode;
	public DBApp() throws IOException {
		tables = new Vector<Table>();
		files = new Hashtable<String, File>();
		File f =new File("resources/DBApp.config");
		FileInputStream fis = new FileInputStream(f);
		Properties p = new Properties();
		p.load(fis);
		 MaximumRowsCountinTablePage = p.getProperty("MaximumRowsCountinTablePage");
		 MaximumEntriesinOctreeNode = p.getProperty("MaximumEntriesinOctreeNode");
	}

	public void init( ){

	}

	static String getAlphaNumericString()
	{
		int n = 20;
		// choose a Character random from this String
		String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
				+ "0123456789"
				+ "abcdefghijklmnopqrstuvxyz";

		// create StringBuffer size of AlphaNumericString
		StringBuilder sb = new StringBuilder(n);

		for (int i = 0; i < n; i++) {

			// generate a random number between
			// 0 to AlphaNumericString variable length
			int index
					= (int)(AlphaNumericString.length()
					* Math.random());

			// add Character one by one in end of sb
			sb.append(AlphaNumericString
					.charAt(index));
		}

		return sb.toString();
	}

	public void createTable(String tableName,String clusteringKey, Hashtable<String,String> ColNameType,  
		Hashtable<String, String> ColNameMin,Hashtable<String,String> ColNameMax) throws DBAppException
	{
		Table table =new Table(tableName,clusteringKey,ColNameType,ColNameMin,ColNameMax);
		tables.add(table);
		serializeTable(table, tableName+".class");
		File f = new File(tableName);
		f.mkdir();
	}

	public void serialize(Page p, String fileName) {
		try {
			File f = new File(fileName);
			files.put(fileName, f);
			FileOutputStream fileOutputStream = new FileOutputStream(fileName);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
			objectOutputStream.writeObject(p);
			objectOutputStream.close();
			fileOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Page deserialize(String fileName){
		Page p = null;
		try {
			FileInputStream fileInputStream = new FileInputStream(fileName);
			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
			p = (Page) objectInputStream.readObject();
			objectInputStream.close();
			fileInputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return p;
	}

	public void serializeTable(Table p, String fileName) {
		try {
			File f = new File(fileName);
			files.put(fileName, f);
			FileOutputStream fileOutputStream = new FileOutputStream(fileName);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
			objectOutputStream.writeObject(p);
			objectOutputStream.close();
			fileOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Table deserializeTable(String fileName) throws DBAppException {
		Table p = null;
		try {
			FileInputStream fileInputStream = new FileInputStream(fileName);
			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
			p = (Table) objectInputStream.readObject();
			objectInputStream.close();
			fileInputStream.close();
		} catch (IOException e) {
			throw new DBAppException();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return p;
	}

	public void insertIntoTable(String strTableName,
								Hashtable<String,Object> htblColNameValue)
			throws DBAppException {
//		Table table = null;
//		Boolean found = false;
//		for(int i = 0; i< tables.size(); i++) {
//
//			if(tables.get(i).getName().equals(strTableName)) {
//				found = true;
//				table = tables.get(i);
//				break;
//			}
//		}
		Table table = deserializeTable(strTableName+".class");

//		if(!found) {
//			throw new DBAppException();
//		}

		if(table.rows.isEmpty()) {
			Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
			table.rows.add(page);
			page.tuples.add(htblColNameValue);
			String fileName = strTableName+"/"+getAlphaNumericString();
			fileName+=".class";

			serialize(page, fileName);
			table.serializedFilesName.add(fileName);
//			String fileName = strTableName + "page" + 0 + ".class";
//			serialize(page, fileName);
			serializeTable(table, table.getName()+".class");
			return;
		}



		String pk = table.getPK();
		boolean test = false;

		//for the case that the insertion is the largest in the table
		int maxPage = table.rows.size()-1;
		String f = table.serializedFilesName.get(maxPage);

		Page max = deserialize(f);
		Hashtable<String,Object> maxTuple = max.tuples.get(max.tuples.size()-1);
		if(maxTuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) < 0) {
			if(max.tuples.size() == Integer.parseInt(MaximumRowsCountinTablePage)) {
				Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
				page.tuples.add(htblColNameValue);
				table.rows.add(page);
				//int num = newCount1 + 1;/////////////////////////////////////////////////////////////////////////////////////////
				String fileName = strTableName+"/"+getAlphaNumericString();
				fileName+=".class";
				serialize(page, fileName);
				table.serializedFilesName.add(fileName);
				serializeTable(table, table.getName()+".class");
				return;
			}
			else {
				max.tuples.add(htblColNameValue);
				serialize(max, f);////////////////////////////////////////////////////
				serializeTable(table, table.getName()+".class");
				return;
			}
		}

		for(int j = 0;j<table.rows.size();j++ ) {

			String fil = table.serializedFilesName.get(j);
			Page p = deserialize(fil);


			//Page p = deserialize(strTableName + "page" + j + ".class");
//			Page p = table.rows.get(j);
			int count = 0;
//			if(p.tuples.isEmpty()) {
//				p.tuples.insertElementAt(htblColNameValue, 0);
//			}
			int start = 0;
			int end = p.tuples.size() - 1;
			while(start <= end) {
				int mid = (start + end) / 2;
				Hashtable<String,Object> tuple = p.tuples.get(mid);
				if (tuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) < 0) {
					start = mid + 1;
				}
				else if (tuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) == 0) {
					throw new DBAppException();
				}
				else {
					if(j==0) {
						p.tuples.insertElementAt(htblColNameValue, mid);
						serialize(p, fil);///////////////////////////////////////
						serializeTable(table, table.getName()+".class");
						redistributeIns(table);
						test = true;
						break;
					}
					else {
//						int w = newCount-1;//////////////////////////////////////////////////////////////////////////////////
						String fi = table.serializedFilesName.get(j-1);
						Page beforeMe = deserialize(fi);
						if(beforeMe.tuples.size() < Integer.parseInt(MaximumRowsCountinTablePage)) {
							beforeMe.tuples.add(htblColNameValue);
							serialize(beforeMe, fi);
							serializeTable(table, table.getName()+".class");
							test = true;
							break;
						}
						else {
							p.tuples.insertElementAt(htblColNameValue, mid);
							serialize(p, fil);///////////////////////////////////////////////
							serializeTable(table, table.getName()+".class");
							redistributeIns(table);
							test = true;
							break;
						}

					}
					//end = mid - 1;
				}
			}
//			for(int i = 0;i < p.tuples.size();i++) {
//				Hashtable<String,Object> tuple = p.tuples.get(i);
//				if(tuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) > 0) {
//					if(j==0) {
//						p.tuples.insertElementAt(htblColNameValue, i);
//						serialize(p, strTableName+"page"+j+".class");
//						redistributeIns(table);
//						test = true;
//						break;
//					}
//					else {
//						int w = j-1;
//						Page ably = deserialize(strTableName + "page" + w + ".class");
//						if(ably.tuples.size() < Integer.parseInt(MaximumRowsCountinTablePage)) {
//							ably.tuples.add(htblColNameValue);
//							serialize(ably, strTableName + "page" + w + ".class");
//							System.out.println("hi");
//							test = true;
//							break;
//						}
//						else {
//							p.tuples.insertElementAt(htblColNameValue, i);
//							serialize(p, strTableName+"page"+j+".class");
//							redistributeIns(table);
//							test = true;
//							break;
//						}
//
//					}
//
//				}
//				count ++;
//			}
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

//		Hashtable<Integer,Integer > eachPageSizeAfter = new Hashtable<Integer, Integer>();
//		for(int x2 = 0;x2<table.rows.size();x2++) {
//			eachPageSizeAfter.put(x2, table.rows.get(x2).tuples.size());
//		}
//
//		if(eachPageSizeBefore.equals(eachPageSizeAfter)) {
//			if(table.rows.get(table.rows.size()-1).tuples.size() == Integer.parseInt(MaximumRowsCountinTablePage)) {
//				Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
//				page.tuples.add(htblColNameValue);
//				table.rows.add(page);
//			}
//			else {
//				table.rows.get(table.rows.size()-1).tuples.add(htblColNameValue);
//			}
//		}

	}

	public void redistributeIns(Table table) {
		boolean findPage = false;
		for(int i=0;i<table.rows.size();i++)
		{
			String f = table.serializedFilesName.get(i);
			Page p = deserialize(f);
			//Page p = deserialize(table.getName()+"page"+i+".class");
			if(p.tuples.size()>Integer.parseInt(MaximumRowsCountinTablePage))
			{
				if(i==table.rows.size()-1)
				{
					Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
					table.rows.add(page);
					String fileName = table.getName()+"/"+getAlphaNumericString();
					fileName+=".class";
//					serialize(page, fileName);
					table.serializedFilesName.add(fileName);
//					page.tuples.add(table.rows.get(i).tuples.get(table.rows.get(i).tuples.size()-1));
//					table.rows.get(i).tuples.remove(table.rows.get(i).tuples.size()-1);

					page.tuples.add(p.tuples.get(p.tuples.size()-1));
					p.tuples.remove(p.tuples.size()-1);

					//int num = newCount+1;/////////////////////////////////////////////////////////////////////////////////
					//String fi = table.serializedFilesName.get(i+1);
					serialize(page, fileName);

					Page erase = null;
					serialize(erase, f);///////////////////////////////////////////////
					serialize(p, f);//////////////////////////////////////////////////////
					serializeTable(table, table.getName()+".class");
				}
				else
				{
					//table.rows.get(i+1).tuples.insertElementAt(table.rows.get(i).tuples.get(table.rows.get(i).tuples.size()-1),0);
					//table.rows.get(i).tuples.remove(table.rows.get(i).tuples.size()-1);
					//int n = newCount+1;//////////////////////////////////////////////////////////////////////////////////////////////////
					String fi = table.serializedFilesName.get(i+1);
					Page next = deserialize(fi);
					next.tuples.insertElementAt(p.tuples.get(p.tuples.size()-1), 0);
					p.tuples.remove(p.tuples.size()-1);

					Page erase = null;
					serialize(erase, f);////////////////////////////////////////////////////
					serialize(erase, fi);
					serialize(p, f);////////////////////////////////////////////////////////////////////
					serialize(next,fi);
					serializeTable(table, table.getName()+".class");
				}
			}
		}
	}

	public void updateTable(String strTableName,
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue )
			throws DBAppException {


		Table table = deserializeTable(strTableName+".class");


		String pk = table.getPK();
		boolean done = false;

		for(int i = 0; i < table.rows.size();i++) { //make sure that we use binary search on content of pages not on pages themselves
			String f = table.serializedFilesName.get(i);
			Page p = deserialize(f);

			int first1 = 0;
			int last1 = p.tuples.size()-1;
			int mid1 = (first1 + last1)/2;
			while(first1 <= last1) {
				Hashtable<String,Object> tuple = p.tuples.get(mid1);
				Object val = tuple.get(pk); // tuple elly fy el table
				if(val.toString().compareTo(strClusteringKeyValue) < 0) {
					first1 = mid1 + 1;
				}
				else if(val.toString().compareTo(strClusteringKeyValue) == 0) {
					int finalMid = mid1;
					Page finalP = p;
					p.tuples.get(mid1).forEach((k, v) ->{
						htblColNameValue.forEach((k2, v2) ->{
							if(k.compareTo(k2) == 0) {
								finalP.tuples.get(finalMid).replace(k, v2);
							}
						});
					});
					Page erase = null;
					serialize(erase, f);///////////////////////////////////////////////////////////////
					serialize(p, f);//////////////////////////////////////////////////////////////////
					serializeTable(table, table.getName()+".class");
					done = true;
					break;
				}
				else {
					last1 = mid1 - 1;
				}
				mid1 = (first1 + last1)/2;
			}
			if(done) break;
		}
	}

	public void deleteFromTable(String strTableName,
								Hashtable<String,Object> htblColNameValue) throws DBAppException {
//		Table table = null;
//		Boolean found = false;
////		for(int i = 0; i< tables.size(); i++) {
////
////			if(tables.get(i).getName().equals(strTableName)) {
////				found = true;
////				table = tables.get(i);
////				break;
////			}
////		}
//		int l = 0, r = tables.size()-1;
//		while (l <= r) {
//			int m = l + (r - l) / 2;
//
//			//int res = strTableName.compareTo(String.valueOf(tables.get(m).getName().equals(strTableName)));
//			int res = tables.get(m).getName().compareTo(strTableName);
//			// Check if x is present at mid
//			if (res == 0){
//				found =true;
//				table =tables.get(m);
//				break;}
//			// If x greater, ignore left half
//			if (res > 0)
//				l = m + 1;
//
//				// If x is smaller, ignore right half
//			else
//				r = m - 1;
//		}
//		if (!found) {
//			throw new DBAppException();
//		}

		Table table = deserializeTable(strTableName + ".class");

		String pk = table.getPK();

		if((htblColNameValue.containsKey(pk))) {
//			boolean findPage = false;

			for(int i = 0; i < table.rows.size();i++) { //make sure that we use binary search on content of pages not on pages themselves
//				Page p = null;
//				int newCount = i;
//				while(!findPage) {
//					File f = new File(strTableName + "page" + newCount + ".class");
//					p = deserialize(strTableName + "page" + newCount + ".class");
//					if(!p.tuples.isEmpty()) {
//						findPage = true;
//
//						break;
//					}
//					newCount++;
//				}
//				findPage = false;

				String f = table.serializedFilesName.get(i);
				Page p = deserialize(f);




				// Page p = deserialize(strTableName + "page" + i + ".class");
				//Page p = table.rows.get(i);
				int first1 = 0;
				int last1 = p.tuples.size()-1;
				int mid1 = (first1 + last1)/2;
				while(first1 <= last1) {
					Hashtable<String,Object> tuple = p.tuples.get(mid1);
					Object val = tuple.get(pk); // tuple elly fy el table
					if(val.toString().compareTo(htblColNameValue.get(pk).toString()) < 0) {
						first1 = mid1 + 1;
					}
					else if(val.toString().compareTo(htblColNameValue.get(pk).toString()) == 0) {
						AtomicInteger countKeys = new AtomicInteger();
						htblColNameValue.forEach((k, v)->{
							if(tuple.get(k).equals(v)) {
								countKeys.getAndIncrement();
							}
						});

						if(countKeys.get() == htblColNameValue.size()) {
							p.tuples.remove(mid1);
							if(p.tuples.isEmpty()) {
								table.rows.remove(i);//////////////////////////////////////////////////////////////////
								table.serializedFilesName.remove(i);
								File fe = new File(f);/////////////////////////////////////////
								fe.delete();
							}
							else {
								serialize(p, f);////////////////////////////////////////////////////////
							}
							serializeTable(table, table.getName()+".class");
							return;
						}

					}
					else {
						last1 = mid1 - 1;
					}
					mid1 = (first1 + last1)/2;
				}
			}
		}


		for(int i = 0; i < table.rows.size();i++) { //make sure that we use binary search on content of pages not on pages themselves

			String f = table.serializedFilesName.get(i);
			Page p = deserialize(f);



			for(int j = 0; j< p.tuples.size();j++) {
				int finalJ = j;
				AtomicInteger countKeys = new AtomicInteger();
				Page finalP = p;
				htblColNameValue.forEach((k, v) ->{

					if(finalP.tuples.get(finalJ).get(k).toString().compareTo(v.toString()) == 0) {
						countKeys.getAndIncrement();
					}
				});
				if(countKeys.get() == htblColNameValue.size()){
					p.tuples.remove(j);
					if(p.tuples.isEmpty()) {
						table.rows.remove(i);
						table.serializedFilesName.remove(i);
						File fe = files.get(f);///////////////////////////////
						fe.delete();
					}
					else {
						serialize(p, f);
					}


					serializeTable(table, table.getName()+".class");

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
		//dbApp.createTable( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
//
//		Hashtable htblColNameValue5 = new Hashtable( );
//		htblColNameValue5.put("id", new Integer( 2343429 ));
//		htblColNameValue5.put("name", new String("zoz" ) );
//		htblColNameValue5.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue5 );
//
//		Hashtable htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 2343432 ));
//		htblColNameValue.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//		Hashtable htblColNameValue1 = new Hashtable( );
//		htblColNameValue1.put("id", new Integer( 2343433 ));
//		htblColNameValue1.put("name", new String("youssef" ) );
//		htblColNameValue1.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue1 );
//
//		Hashtable htblColNameValue2 = new Hashtable( );
//		htblColNameValue2.put("id", new Integer( 2343431 ));
//		htblColNameValue2.put("name", new String("Ahmed" ) );
//		htblColNameValue2.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue2 );
//
//		Hashtable htblColNameValue3 = new Hashtable( );
//		htblColNameValue3.put("id", new Integer( 2343430 ));
//		htblColNameValue3.put("name", new String("mohamed" ) );
//		htblColNameValue3.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue3 );

//
//
//		Hashtable htblColNameVal = new Hashtable( );
//		htblColNameVal.put("id", new Integer( 2343431 ));
//		htblColNameVal.put("name", new String("Ahmed" ) );
//		htblColNameVal.put("gpa", new Double( 0.95 ) );
//		dbApp.deleteFromTable( strTableName , htblColNameVal );
////
//////		Hashtable htblColNameValue32 = new Hashtable( );
//////		htblColNameValue32.put("id", new Integer( 2343431 ));
//////		htblColNameValue32.put("name", new String("mohame" ) );
//////		htblColNameValue32.put("gpa", new Double( 0.95 ) );
//////		dbApp.insertIntoTable( strTableName , htblColNameValue32 );
//////
//		Hashtable htblColNameVa = new Hashtable( );
//		htblColNameVa.put("id", new Integer( 2343432 ));
////		htblColNameVa.put("name", new String("test was ahmed noor" ) );
////		htblColNameVa.put("gpa", new Double( 0.95 ) );
//		dbApp.deleteFromTable( strTableName , htblColNameVa );
//
//
//
////		Hashtable htblColNameValue5 = new Hashtable( );
////		htblColNameValue5.put("id", new Integer( 2343429 ));
////		htblColNameValue5.put("name", new String("zoz" ) );
////		htblColNameValue5.put("gpa", new Double( 0.95 ) );
////		dbApp.insertIntoTable( strTableName , htblColNameValue5 );
////
//		Hashtable update = new Hashtable( );
//		update.put("name", new String("test was Ahmed" ) );
//		update.put("gpa", new Double(2.0) );
//		dbApp.updateTable(strTableName, "2343431", update);
////
//		Hashtable htblColNameValu = new Hashtable( );
//		htblColNameValu.put("name", new String("test was ahmed noor" ) );
//		htblColNameValu.put("gpa", new Double(2.0 ) );
//		dbApp.deleteFromTable( strTableName , htblColNameValu );
////
//		Hashtable htblColNameV = new Hashtable( );
//		htblColNameV.put("name", new String("Ahmed" ) );
//		dbApp.deleteFromTable( strTableName , htblColNameV );
//
//		Hashtable htblColNameValue4 = new Hashtable( );
//		htblColNameValue4.put("id", new Integer( 2343434 ));
//		htblColNameValue4.put("name", new String("zizooo" ) );
//		htblColNameValue4.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue4 );
//
//		Hashtable htblColNameValue66 = new Hashtable( );
//		htblColNameValue66.put("id", new Integer( 2343435 ));
//		htblColNameValue66.put("name", new String("yarab" ) );
//		htblColNameValue66.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue66 );


		Table t = dbApp.deserializeTable(strTableName+".class");
		System.out.println(t.rows.size());
		for(int i = 0;i < t.rows.size();i++) {

			String f = t.serializedFilesName.get(i);
			Page p = dbApp.deserialize(f);
			//Page p = t.rows.get(i);
			for(int j = 0;j<p.tuples.size();j++) {
				Hashtable<String,Object> h = p.tuples.get(j);
				System.out.println(h);
			}
			System.out.println();
		}
		System.out.println();
	}



}
