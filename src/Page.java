import java.util.Hashtable;
import java.util.Vector;
import java.io.Serializable;

public class Page implements java.io.Serializable {
	int maximumRows;
	Vector<Vector<Hashtable<Object,Object>>> tuples;
	public Page(int maximumRows) {
		this.maximumRows=maximumRows;
	}

	public int getMaximumRows() {
		return maximumRows;
	}

	public void setMaximumRows(int maximumRows) {
		this.maximumRows = maximumRows;
	}
}
