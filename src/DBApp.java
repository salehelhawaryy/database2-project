import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class DBApp {

	boolean ranOnce = false;

	String MaximumRowsCountinTablePage;
	String MaximumEntriesinOctreeNode;
	public DBApp() {

	}
	public void init( ) {

	}

	public void readConfig() throws DBAppException {
		File f =new File("resources/DBApp.config");
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(f);
		} catch (FileNotFoundException e) {
			throw new DBAppException();
		}
		Properties p = new Properties();
		try {
			p.load(fis);
		} catch (IOException e) {
			throw new DBAppException();
		}
		MaximumRowsCountinTablePage = p.getProperty("MaximumRowsCountinTablePage");
		MaximumEntriesinOctreeNode = p.getProperty("MaximumEntriesinOctreeNode");
	}

	public static Vector<Vector<String>> readCSV() throws DBAppException {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("resources/"+"metadata.csv"));
		} catch (FileNotFoundException e) {
			throw new DBAppException();
		}
		String line = null;
		try {
			line = br.readLine();
		} catch (IOException e) {
			throw new DBAppException();
		}
		Vector<Vector<String>> vecvec = new Vector<>();
		int c = 0;
		while (line != null) {
			String[] content = line.split(",");
			if (c == 0) {
				c++;
				try {
					line = br.readLine();
				} catch (IOException e) {
					throw new DBAppException();
				}
				continue;
			}
			Vector<String> vec = new Vector<>(Arrays.asList(content));
			vecvec.add(vec);

			try {
				line = br.readLine();
			} catch (IOException e) {
				throw new DBAppException();
			}
		}
		try {
			br.close();
		} catch (IOException e) {
			throw new DBAppException();
		}
		return vecvec;
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

	public void createTable(String strTableName,String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
		Hashtable<String, String> htblColNameMin,Hashtable<String,String> htblColNameMax) throws DBAppException {

		for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
			String k = entry.getKey();
			String v = entry.getValue();
			if (v.toLowerCase().compareTo("java.lang.double") != 0 && v.toLowerCase().compareTo("java.lang.integer") != 0 &&
					v.toLowerCase().compareTo("java.lang.string") != 0 && v.toLowerCase().compareTo("java.util.date") != 0) {
				throw new DBAppException();
			}
		}

		File fq = new File("resources/data/"+strTableName+".class");
		if(fq.exists()) {
			throw new DBAppException();
		}


		String Colname;
		String Coltype;
		String Clusterkey="false";
		String Min;
		String Max;
		Table table = new Table(strTableName,strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
		serializeTable(table, "resources/data/"+strTableName+".class");
		File f = new File("resources/data/"+strTableName);
		f.mkdir();

		Enumeration<String> enu = htblColNameType.keys();

		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new FileWriter("resources/"+"metadata.csv", true));
		} catch (IOException e) {
			throw new DBAppException();
		}
		StringBuilder sb = new StringBuilder();

		if (!ranOnce) {
			ranOnce = true;
			sb.append("Table Name");
			sb.append(",");
			sb.append("Column Name");
			sb.append(",");
			sb.append("Column Type");
			sb.append(",");
			sb.append("Clusterkey");
			sb.append(",");
			sb.append("IndexName");
			sb.append(",");
			sb.append("IndexType");
			sb.append(",");
			sb.append("min");
			sb.append(",");
			sb.append("max");
			sb.append("\r\n");
		}

		while (enu.hasMoreElements()) {
			Colname = enu.nextElement().toString();
			Coltype = htblColNameType.get(Colname);
			if (Colname == strClusteringKeyColumn) {
				Clusterkey = "true";
			}
			Min = htblColNameMin.get(Colname);
			Max = htblColNameMax.get(Colname);
			sb.append(strTableName);
			sb.append(",");
			sb.append(Colname);
			sb.append(",");
			sb.append(Coltype);
			sb.append(",");
			sb.append(Clusterkey);
			sb.append(",");
			sb.append("null");
			sb.append(",");
			sb.append("null");
			sb.append(",");
			sb.append(Min);
			sb.append(",");
			sb.append(Max);
			sb.append("\r\n");
		}
		pw.flush();
		pw.write(sb.toString());
		pw.close();
	}

	public void serialize(Page p, String fileName) throws DBAppException {
		try {
			File f = new File(fileName);
			FileOutputStream fileOutputStream = new FileOutputStream(fileName);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
			objectOutputStream.writeObject(p);
			objectOutputStream.close();
			fileOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException();
		}
	}

	public Page deserialize(String fileName) throws DBAppException {
		Page p = null;
		try {
			FileInputStream fileInputStream = new FileInputStream(fileName);
			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
			p = (Page) objectInputStream.readObject();
			objectInputStream.close();
			fileInputStream.close();
		} catch (IOException e) {
			throw new DBAppException();
		} catch (ClassNotFoundException e) {
			throw new DBAppException();
		}
		return p;
	}

	public void serializeTable(Table p, String fileName) throws DBAppException {
		try {
			File f = new File(fileName);
			FileOutputStream fileOutputStream = new FileOutputStream(fileName);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
			objectOutputStream.writeObject(p);
			objectOutputStream.close();
			fileOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException();
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
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException();
		}
		return p;
	}

	public void insertIntoTable(String strTableName,
								Hashtable<String,Object> htblColNameValue)
			throws DBAppException {



		Table table = deserializeTable("resources/data/"+strTableName+".class");



//		Vector<Vector<String>> vecvec = readCSV();
		boolean check = checkcallname(strTableName,htblColNameValue);
		boolean check1 = checkDataType(strTableName, htblColNameValue);

		if (!check) {
			throw new DBAppException();
		}
		if (!check1) {
			throw new DBAppException();
		}

		if(!htblColNameValue.containsKey(table.getPK())) {
			throw new DBAppException();
		}

		table.colNameType.forEach((k,v)->{
			if(!htblColNameValue.containsKey(k)) {
				String w = "null";
				htblColNameValue.put(k, w);
			}
		});

		readConfig();

		if(table.rows.isEmpty()) {
			Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
			table.rows.add(page);
			page.tuples.add(htblColNameValue);
			String fileName = "resources/data/"+strTableName+"/"+getAlphaNumericString();
			fileName+=".class";
			while(table.serializedFilesName.contains(fileName)) {
				fileName = "resources/data/"+strTableName+"/"+getAlphaNumericString();
				fileName+=".class";
			}
			serialize(page, fileName);
			table.serializedFilesName.add(fileName);
			serializeTable(table, "resources/data/"+table.getName()+".class");
			page = null;
			System.gc();
			return;
		}

		String pk = table.getPK();
		boolean test = false;

		int maxPage = table.rows.size()-1;
		String f = table.serializedFilesName.get(maxPage);
		readConfig();
		Page max = deserialize(f);
		Hashtable<String,Object> maxTuple = max.tuples.get(max.tuples.size()-1);
		if(maxTuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) < 0) {
			if(max.tuples.size() == Integer.parseInt(MaximumRowsCountinTablePage)) {
				Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
				page.tuples.add(htblColNameValue);
				table.rows.add(page);
				//int num = newCount1 + 1;/////////////////////////////////////////////////////////////////////////////////////////
				String fileName = "resources/data/"+strTableName+"/"+getAlphaNumericString();
				fileName+=".class";
				while(table.serializedFilesName.contains(fileName)) {
					fileName = "resources/data/"+strTableName+"/"+getAlphaNumericString();
					fileName+=".class";
				}
				serialize(page, fileName);
				table.serializedFilesName.add(fileName);
				serializeTable(table, "resources/data/"+table.getName()+".class");
				page = null;
				max = null;
				System.gc();
				return;
			}
			else {
				max.tuples.add(htblColNameValue);
				serialize(max, f);////////////////////////////////////////////////////
				serializeTable(table, "resources/data/"+table.getName()+".class");
				max = null;
				System.gc();
				return;
			}
		}
		max = null;
		System.gc();

		for(int j = 0;j<table.rows.size();j++ ) {
			String fil = table.serializedFilesName.get(j);
			Page p = deserialize(fil);
			int count = 0;
			int start = 0;
			int end = p.tuples.size() - 1;
			for (int i = 0;i<p.tuples.size();i++){
				int mid = (start + end) / 2;
				Hashtable<String, Object> tuple = p.tuples.get(i);
				if (tuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) < 0) {
					start = mid + 1;
				} else if (tuple.get(pk).toString().compareTo(htblColNameValue.get(pk).toString()) == 0) {
					throw new DBAppException();
				} else {
					if (j == 0) {
						p.tuples.insertElementAt(htblColNameValue, i);
						serialize(p, fil);///////////////////////////////////////
						serializeTable(table, "resources/data/"+table.getName() + ".class");
						p = null;
						System.gc();
						redistributeIns(table);
						test = true;
						break;
					} else {
//						int w = newCount-1;//////////////////////////////////////////////////////////////////////////////////
						String fi = table.serializedFilesName.get(j - 1);
						readConfig();
						Page beforeMe = deserialize(fi);
						if (beforeMe.tuples.size() < Integer.parseInt(MaximumRowsCountinTablePage)) {
							beforeMe.tuples.add(htblColNameValue);
							serialize(beforeMe, fi);
							serializeTable(table, "resources/data/"+table.getName() + ".class");
							test = true;
							p = null;
							beforeMe = null;
							System.gc();
							break;
						} else {
							p.tuples.insertElementAt(htblColNameValue, i);
							serialize(p, fil);///////////////////////////////////////////////
							serializeTable(table, "resources/data/"+table.getName() + ".class");
							redistributeIns(table);
							test = true;
							p = null;
							System.gc();
							break;
						}

					}
				}
			}
			if (test) {
				break;
			}
			p = null;
			System.gc();
		}
	}


	private boolean checkDataType(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		boolean check=false;
		Vector<Vector<String>> vecvec = readCSV();
		String[] colname = new String[htblColNameValue.keySet().toArray().length];
		for (int j = 0; j < htblColNameValue.keySet().toArray().length; j++) {
			colname[j] = (String) htblColNameValue.keySet().toArray()[j];
			//System.out.println((String) htblColNameValue.keySet().toArray()[j]);
		}
//		String[] colname = (String[]) htblColNameValue.keySet().toArray();

		for (int j = 0; j < colname.length; j++) {
			check=false;
			String current=colname[j];
			Object value = htblColNameValue.get(current);
			for (int k = 0; k < vecvec.size() ; k++) {
				if(strTableName.equals(vecvec.get(k).get(0))){
					if(current.equals(vecvec.get(k).get(1))){
						switch (vecvec.get(k).get(2).toLowerCase()) {
							case "java.lang.double" :
								if(!(value instanceof Double))
									return false;
								if(Double.parseDouble(vecvec.get(k).get(6)) > (Double.parseDouble(value.toString())) || Double.parseDouble(vecvec.get(k).get(7)) < (Double.parseDouble(value.toString()))) {
									return false;
								}
								break;
							case "java.lang.string":
								if(!(value instanceof String))
									return false;
								if((vecvec.get(k).get(6).compareTo(value.toString().toLowerCase()) > 0) || vecvec.get(k).get(7).compareTo(value.toString().toLowerCase()) < 0) {
									System.out.println("Min:"+vecvec.get(k).get(6));
									System.out.println("Max:"+vecvec.get(k).get(7));
									System.out.println("Value:"+value.toString());
									System.out.println(" "+ vecvec.get(k).get(7).compareTo(value.toString())+" "+ vecvec.get(k).get(6).compareTo(value.toString()));
									return false;
								}
								break;
							case "java.lang.integer":
								if(!(value instanceof Integer))
									return false;
								if(Integer.parseInt(vecvec.get(k).get(6)) > (Integer.parseInt(value.toString())) || Integer.parseInt(vecvec.get(k).get(7)) < (Integer.parseInt(value.toString()))) {
								return false;
								}
								break;
							case "java.util.date":
								if(!(value instanceof  java.util.Date))
									return false;
								if((vecvec.get(k).get(6).compareTo(value.toString()) > 0) || vecvec.get(k).get(7).compareTo(value.toString()) < 0) {
									return false;
								}
								break;
							default:
								break;
						}
						//break;
					}

				}
			}
//			if(!check)
//				return false;
		}
		return true;
	}

	private boolean checkcallname(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		boolean check=false;
		Vector<Vector<String>> vecvec = readCSV();
		int i =0;

		String[] colname = new String[htblColNameValue.keySet().toArray().length];
		for (int j = 0; j < htblColNameValue.keySet().toArray().length; j++) {
			colname[j] = (String) htblColNameValue.keySet().toArray()[j];
			//System.out.println((String) htblColNameValue.keySet().toArray()[j]);
		}
//		String[] colname = (String[]) htblColNameValue.keySet().toArray();

		for (int j = 0; j < colname.length; j++) {
			check=false;
			String current=colname[j];
			for (int k = 0; k < vecvec.size() ; k++) {

				if(strTableName.equals(vecvec.get(k).get(0))){
					if(current.equals(vecvec.get(k).get(1))){
						check=true;
						break;
					}

				}
			}
			if(!check)
				return false;
		}
		return true;

	}


	public void redistributeIns(Table table) throws DBAppException {
		for(int i=0;i<table.rows.size();i++)
		{
			String f = table.serializedFilesName.get(i);
			Page p = deserialize(f);
			//Page p = deserialize(table.getName()+"page"+i+".class");
			readConfig();
			if(p.tuples.size()>Integer.parseInt(MaximumRowsCountinTablePage))
			{
				if(i==table.rows.size()-1)
				{
					Page page = new Page(Integer.parseInt(MaximumRowsCountinTablePage));
					table.rows.add(page);
					String fileName = "resources/data/"+table.getName()+"/"+getAlphaNumericString();
					fileName+=".class";
					while(table.serializedFilesName.contains(fileName)) {
						fileName = "resources/data/"+table.getName()+"/"+getAlphaNumericString();
						fileName+=".class";
					}
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
					serializeTable(table, "resources/data/"+table.getName()+".class");
					page = null;
					p = null;
					System.gc();
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
					serializeTable(table, "resources/data/"+table.getName()+".class");
					p = null;
					next = null;
					System.gc();
				}
			}
			p = null;
			System.gc();
		}
	}

	public void updateTable(String strTableName,
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue )
			throws DBAppException {


		Table table = deserializeTable("resources/data/"+strTableName+".class");

		if(htblColNameValue.containsKey(table.getPK()) || strClusteringKeyValue.isEmpty()) {
			throw new DBAppException();
		}



		boolean check = checkcallname(strTableName,htblColNameValue);
		boolean check1 = checkDataType(strTableName, htblColNameValue);

		if (!check) {
			throw new DBAppException();
		}
		if (!check1) {
			throw new DBAppException();
		}


		String pk = table.getPK();
		boolean done = false;
		boolean updated = false;

		for(int i = 0; i < table.rows.size();i++) {
			String f = table.serializedFilesName.get(i);
			Page p = deserialize(f);

			int first1 = 0;
			int last1 = p.tuples.size()-1;
			int mid1 = (first1 + last1)/2;
			while(first1 <= last1) {
				Hashtable<String,Object> tuple = p.tuples.get(mid1);
				Object val = tuple.get(pk); // tuple elly fy el table
				int compare = 0;
				if(val instanceof Double) {
					Double strClust = Double.parseDouble(strClusteringKeyValue);
					compare = Double.compare((Double) val, strClust);
				} else if(val instanceof Integer) {
					Integer strClust = Integer.parseInt(strClusteringKeyValue);
					compare = Integer.compare((Integer) val, strClust);
				} else if (val instanceof String) {
					compare = val.toString().compareTo(strClusteringKeyValue);
				} else {
					SimpleDateFormat formatter2=new SimpleDateFormat("YYYY-MM-DD");
					Date strClust;
					try {
						strClust = formatter2.parse(strClusteringKeyValue);
					} catch (ParseException e) {
						throw new DBAppException();
					}
					compare = ((Date)val).compareTo(strClust);
				}
				if(compare < 0) {
					first1 = mid1 + 1;
				}
				else if(compare == 0) {
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
					serializeTable(table, "resources/data/"+table.getName()+".class");

					p = null;
					System.gc();
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


		Table table = deserializeTable("resources/data/"+strTableName + ".class");

		boolean check = checkcallname(strTableName,htblColNameValue);
		boolean check1 = checkDataType(strTableName, htblColNameValue);

		if (!check) {
			throw new DBAppException();
		}
		if (!check1) {
			throw new DBAppException();
		}

		String pk = table.getPK();

		if((htblColNameValue.containsKey(pk))) {

			for(int i = 0; i < table.rows.size();i++) { //make sure that we use binary search on content of pages not on pages themselves


				String f = table.serializedFilesName.get(i);
				Page p = deserialize(f);

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
							serializeTable(table, "resources/data/"+table.getName()+".class");
							p = null;
							System.gc();
							return;
						}

					}
					else {
						last1 = mid1 - 1;
					}
					mid1 = (first1 + last1)/2;
				}
				p = null;
				System.gc();
			}
		}


		for(int i = 0; i < table.rows.size();i++) {

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
					j--;
					if(p.tuples.isEmpty()) {
						table.rows.remove(i);
						table.serializedFilesName.remove(i);
						File fe = new File(f);
						fe.delete();
					}
					else {
						serialize(p, f);
					}
					serializeTable(table, "resources/data/"+table.getName()+".class");
				}
			}
			p = null;
			System.gc();
		}
	}
}
