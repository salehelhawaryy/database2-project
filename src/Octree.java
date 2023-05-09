import java.util.Vector;

public class Octree {
    Vector<OctPoint> points=new Vector<>();
    OctPoint topBoundary,BottomBoundary;
    Octree[] children= new Octree[8];

    int limit;



    public Octree(Object x1,Object y1,Object z1,Object x2,Object y2,Object z2,int limit){
        this.topBoundary=new OctPoint(x1,y1,z1,null);
        this.BottomBoundary=new OctPoint(x2,y2,z2, null);
        limit=limit;
    }

    public void split(){
        Object midx;
        Object midy;
        Object midz;



        children[0]=new Octree()
    }

    public void insert(Object x,Object y,Object z, Object data){
        OctPoint insertion = new OctPoint(x,y,z,data);
        if(points.size()<limit){
            points.add(insertion);
            return;
        }

        split();

        Object midx;
        Object midy;
        Object midz;

        if()

    }



}
