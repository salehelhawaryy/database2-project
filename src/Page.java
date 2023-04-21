import java.util.Hashtable;
import java.util.Vector;
import java.io.Serializable;

public class Page implements java.io.Serializable {
	int maximumRows;
	Vector<Hashtable<String,Object>> tuples;
	private static final long serialVersionUID = 1L;
	public Page(int maximumRows) {
		tuples = new Vector<Hashtable<String, Object>>();
		this.maximumRows=maximumRows;
	}





	public int getMaximumRows() {
		return maximumRows;
	}

	public void setMaximumRows(int maximumRows) {
		this.maximumRows = maximumRows;
	}
}
