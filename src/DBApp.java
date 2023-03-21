import java.util.Hashtable;
import java.util.Vector;

public class DBApp {
	Vector<Table> tables;
	
	public void createTable(String tableName,String clusteringKey, Hashtable<String,String> ColNameType,  
		Hashtable<String, String> ColNameMin,Hashtable<String,String> ColNameMax)
	{
		Table table =new Table(tableName,clusteringKey,ColNameType,ColNameMin,ColNameMax);
		tables.add(table);
	}

	public static void main(String[] args) {
//		System.out.println("test");
	}
}
