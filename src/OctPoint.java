import java.io.Serializable;
import java.util.Vector;

public class OctPoint implements Serializable {
    private static final long serialVersionUID = 1L;

    private Object x;
    private Object y;
    private Object z;

    Object object;

    boolean hasDuplicates=false;


   // private boolean nullify = false;

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public OctPoint(Object x, Object y, Object z, Object object){
        this.x = x;
        this.y = y;
        this.z = z;
        this.object=object;
    }

//    public OctPoint(){
//        nullify = true;
//    }

    public Object getX(){
        return x;
    }

    public Object getY(){
        return y;
    }

    public Object getZ(){
        return z;
    }

//    public boolean isNullified(){
//        return nullify;
//    }
}