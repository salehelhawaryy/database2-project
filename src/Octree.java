import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Octree implements Serializable {
    private static final long serialVersionUID = 1L;
    Vector<OctPoint> points=new Vector<>();
    OctPoint topBoundary,BottomBoundary;
    Octree[] children= new Octree[8];


    boolean canInsert=true;
    int limit;



    public Octree(Object x1,Object y1,Object z1,Object x2,Object y2,Object z2,int limit){
        this.topBoundary=new OctPoint(x1,y1,z1,null);
        this.BottomBoundary=new OctPoint(x2,y2,z2, null);
        this.limit=limit;
    }

//    public String StringMid(String str1, String str2) {
//
//
//        int minLength = Math.min(str1.length(), str2.length());
//        int midpointIndex = minLength / 2;
//
//        String midpoint = str1.substring(0, midpointIndex) + str2.substring(0, midpointIndex);
//        if (minLength % 2 != 0) {
//            midpoint += str1.charAt(midpointIndex);
//        }
//
//        return midpoint; // Output the midpoint string
//    }

    static String StringMid(String S, String T)
    {
        int N = Math.max(S.length(), T.length());
        int lenS = S.length();
        int lenT = T.length();
        int maxLen = Math.max(lenS, lenT);
        StringBuilder sbS = new StringBuilder(S);
        StringBuilder sbT = new StringBuilder(T);

        // Pad the shorter string with leading zeros
        if (lenS < maxLen) {
            for (int i = 0; i < maxLen - lenS; i++) {
                sbS.insert(0, 'a');
            }
        }
        if (lenT < maxLen) {
            for (int i = 0; i < maxLen - lenT; i++) {
                sbT.insert(0, 'a');
            }
        }

        // Stores the base 26 digits after addition
        int[] a1 = new int[maxLen + 1];

        for (int i = 0; i < maxLen; i++) {
            a1[i + 1] = (int)sbS.charAt(i) - 97
                    + (int)sbT.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = maxLen; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= maxLen; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= maxLen) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int)a1[i] / 2;
        }
        String res="";
        for (int i = 1; i <= maxLen; i++) {
//            System.out.print((char)(a1[i] + 97));
            res+=(char)(a1[i] + 97);
        }
        return res;
    }

    public Object dateMid(Object min, Object max) throws DBAppException {
        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");

//        Date date1 = new Date(); // Replace this with your own Date object
//        Date date2 = new Date(); // Replace this with your own Date object
//
//        try {
//            date1 = sdformat.parse("2002-01-01");
//            date2 = sdformat.parse("2004-01-01");
//        } catch (ParseException e) {
//            throw new DBAppException();
//        }

        Date date1 = (Date) min;
        Date date2 = (Date) max;

//        DateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
//
//        String formattedDate = formatter1.format(test);



        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        long diffInDays = ChronoUnit.DAYS.between(date1.toInstant(), date2.toInstant());
        Date midDate = new Date(date1.getTime() + diffInDays / 2 * 24L * 60L * 60L * 1000L);
//        String midDateStr = dateFormat.format(midDate);
//
//        System.out.println(midDateStr); // Output the midpoint date
        return midDate;
    }

    public Object getMid(Object min, Object max) throws DBAppException {
        Object mid = null;
        if(min instanceof String) {
            mid = StringMid((String) min, (String) max);
        } else if(min instanceof Integer) {
            mid = ((Integer) min + (Integer) max) / 2;
        }
        else if(min instanceof Double) {
            mid = ((Double) min + (Double) max) / 2;
        } else {
            mid = dateMid(min, max);
        }
        return mid;
    }

    public int compareObject(Object o1, Object o2) {
        if(o1 instanceof String) {
            String o11 = o1.toString();
            String o22 = o2.toString();
            return (o11.toLowerCase().compareTo(o22.toLowerCase()));
        } else if(o1 instanceof Double) {
            return ((Double) o1).compareTo(((Double)o2));
        } else if(o1 instanceof Integer) {
            return ((Integer) o1).compareTo(((Integer)o2));
        } else {
            return ((Date) o1).compareTo(((Date)o2));
        }
    }

    public void split(OctPoint newInsert) throws DBAppException {
        Object midx;
        Object midy;
        Object midz;

        midx = getMid(topBoundary.getX(), BottomBoundary.getX());
        midy = getMid(topBoundary.getY(), BottomBoundary.getY());
        midz = getMid(topBoundary.getZ(), BottomBoundary.getZ());

        children[0]=new Octree(this.topBoundary.getX(),this.topBoundary.getY(),this.topBoundary.getZ(),midx, midy, midz, this.limit); //topleftfront
        children[1]=new Octree(this.topBoundary.getX(),this.topBoundary.getY(),midz,midx, midy, this.BottomBoundary.getZ(), this.limit); //topleftbottom
        children[2]=new Octree(this.topBoundary.getX(),midy,this.topBoundary.getZ(),midx, this.BottomBoundary.getY(), midz, this.limit); //BottomLeftFront
        children[3]=new Octree(this.topBoundary.getX(),midy,midz,midx, this.BottomBoundary.getY(), this.BottomBoundary.getZ(), this.limit); //BottomLeftBack

        children[4]=new Octree(midx,this.topBoundary.getY(),this.topBoundary.getZ(),this.BottomBoundary.getX(), midy, midz, this.limit);
        children[5]=new Octree(midx,this.topBoundary.getY(),midz,this.BottomBoundary.getX(), midy, this.BottomBoundary.getZ(), this.limit);
        children[6]=new Octree(midx,midy,this.topBoundary.getZ(),this.BottomBoundary.getX(), this.BottomBoundary.getY(), midz, this.limit);
        children[7]=new Octree(midx,midy,midz,this.BottomBoundary.getX(), this.BottomBoundary.getY(), this.BottomBoundary.getZ(), this.limit);

        for (int i = 0; i <= this.points.size(); i++) {
            int pos;
            OctPoint current ;
            if(i==this.points.size())
                current=newInsert;
            else current=this.points.get(i);
            int xCompare = compareObject(current.getX(), midx);
            int yCompare = compareObject(current.getY(), midy);
            int zCompare = compareObject(current.getZ(), midz);
            if(xCompare <= 0){
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos=0;
                    else
                        pos = 1;
                }else{
                    if(zCompare <= 0)
                        pos = 2;
                    else
                        pos = 3;
                }
            }else{
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos = 4;
                    else
                        pos = 5;
                }else {
                    if(zCompare <= 0)
                        pos = 6;
                    else
                        pos = 7;
                }
            }
            children[pos].insert(current.getX(),current.getY(),current.getZ(),current.getObject());
            if(i!=this.points.size()) {
                this.points.remove(current);
                i--;
            }
        }
        this.canInsert=false;
    }

    public void insert(Object x,Object y,Object z, Object data) throws DBAppException {


        if(x instanceof String && !(this.topBoundary.getX() instanceof String)) {
            throw new DBAppException();
        }
        if(x instanceof Double && !(this.topBoundary.getX() instanceof Double)) {
            throw new DBAppException();
        }
        if(x instanceof Date && !(this.topBoundary.getX() instanceof Date)) {
            throw new DBAppException();
        }
        if(x instanceof Integer && !(this.topBoundary.getX() instanceof Integer)) {
            throw new DBAppException();
        }

        if(y instanceof String && !(this.topBoundary.getY() instanceof String)) {
            throw new DBAppException();
        }
        if(y instanceof Double && !(this.topBoundary.getY() instanceof Double)) {
            throw new DBAppException();
        }
        if(y instanceof Date && !(this.topBoundary.getY() instanceof Date)) {
            throw new DBAppException();
        }
        if(y instanceof Integer && !(this.topBoundary.getY() instanceof Integer)) {
            throw new DBAppException();
        }

        if(z instanceof String && !(this.topBoundary.getZ() instanceof String)) {
            throw new DBAppException();
        }
        if(z instanceof Double && !(this.topBoundary.getZ() instanceof Double)) {
            throw new DBAppException();
        }
        if(z instanceof Date && !(this.topBoundary.getZ() instanceof Date)) {
            throw new DBAppException();
        }
        if(z instanceof Integer && !(this.topBoundary.getZ() instanceof Integer)) {
            throw new DBAppException();
        }

        OctPoint insertion = new OctPoint(x,y,z,data);

        if(points.size()<limit && canInsert){
            for(int i=0;i< points.size();i++)
            {
                if(points.get(i).getX().equals(x) && points.get(i).getY().equals(y) && points.get(i).getZ().equals(z))
                {
                    //System.out.println(data+" "+i);
                    if(points.get(i).hasDuplicates) {
                       //System.out.println(points.get(i).getObject());
                        Vector<Object> vec = (Vector) points.get(i).getObject();
                        vec.add(insertion.getObject());
                        return;
                    }
                    else {
                        //System.out.println("el else "+ data+" "+i);
                        Vector<Object> newVec = new Vector<Object>();

                        newVec.add(points.get(i).getObject());
                        newVec.add(insertion.getObject());
                        points.get(i).setObject(newVec);
                        points.get(i).hasDuplicates = true;
                        return;
                    }

                }
            }
            points.add(insertion);
            return;
        }



//        if(points.size()<limit && canInsert){
//
//           points.add(insertion);
//            return;
//        }

        if(this.children[0]==null){
            split(insertion);
        }
        else{
           Object midx = getMid(topBoundary.getX(), BottomBoundary.getX());
           Object midy = getMid(topBoundary.getY(), BottomBoundary.getY());
           Object midz = getMid(topBoundary.getZ(), BottomBoundary.getZ());

           int xCompare = compareObject(insertion.getX(), midx);
           int yCompare = compareObject(insertion.getY(), midy);
           int zCompare = compareObject(insertion.getZ(), midz);
           int pos;

            if(xCompare <= 0){
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos=0;
                    else
                        pos = 1;
                }else{
                    if(zCompare <= 0)
                        pos = 2;
                    else
                        pos = 3;
                }
            }else{
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos = 4;
                    else
                        pos = 5;
                }else {
                    if(zCompare <= 0)
                        pos = 6;
                    else
                        pos = 7;
                }
            }
            children[pos].insert(insertion.getX(),insertion.getY(),insertion.getZ(),insertion.getObject());
        }

        //if()

    }

    public void remove(Object x,Object y,Object z, Object obj) throws DBAppException {
        if(this.canInsert){
            for(int i=0;i<this.points.size();i++){
                Object x_=points.get(i).getX();
                Object y_=points.get(i).getY();
                Object z_=points.get(i).getZ();
                Object obj_ = points.get(i).getObject();
                if(x_.equals(x) && y_.equals(y) && z_.equals(z) && points.get(i).hasDuplicates) {
                    Vector<Object> vec = (Vector) points.get(i).getObject();
                    for (int j = 0; j < vec.size() ; j++) {
                        if(vec.get(j).equals(obj)) {
                            vec.remove(j);
                            j--;
                        }
                    }
                    if(vec.isEmpty()) {
                        points.remove(i);
                    }
                }
                else if(x_.equals(x) && y_.equals(y) && z_.equals(z) && obj_.equals(obj))///////////////rage3 ma3 saleh
                    this.points.remove(this.points.get(i));
            }
        }
        else{
            Object midx = getMid(topBoundary.getX(), BottomBoundary.getX());
            Object midy = getMid(topBoundary.getY(), BottomBoundary.getY());
            Object midz = getMid(topBoundary.getZ(), BottomBoundary.getZ());

            int xCompare = compareObject(x, midx);
            int yCompare = compareObject(y, midy);
            int zCompare = compareObject(z, midz);
            int pos;

            if(xCompare <= 0){
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos=0;
                    else
                        pos = 1;
                }else{
                    if(zCompare <= 0)
                        pos = 2;
                    else
                        pos = 3;
                }
            }else{
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos = 4;
                    else
                        pos = 5;
                }else {
                    if(zCompare <= 0)
                        pos = 6;
                    else
                        pos = 7;
                }
            }
            //Object o = this.children[pos].get(x,y,z);
            this.children[pos].remove(x, y, z, obj);
        }
    }

    public Object removeWithoutObject(Object x,Object y,Object z) throws DBAppException {
        if (this.canInsert) {
            Object res = null;
            for (int i = 0; i < this.points.size(); i++) {
                Object x_ = points.get(i).getX();
                Object y_ = points.get(i).getY();
                Object z_ = points.get(i).getZ();

                if (x_.equals(x) && y_.equals(y) && z_.equals(z)) {
                    res = this.points.get(i).getObject();
                    this.points.remove(this.points.get(i));

                }

            }
            return res;
        } else {
            Object midx = getMid(topBoundary.getX(), BottomBoundary.getX());
            Object midy = getMid(topBoundary.getY(), BottomBoundary.getY());
            Object midz = getMid(topBoundary.getZ(), BottomBoundary.getZ());

            int xCompare = compareObject(x, midx);
            int yCompare = compareObject(y, midy);
            int zCompare = compareObject(z, midz);
            int pos;

            if (xCompare <= 0) {
                if (yCompare <= 0) {
                    if (zCompare <= 0)
                        pos = 0;
                    else
                        pos = 1;
                } else {
                    if (zCompare <= 0)
                        pos = 2;
                    else
                        pos = 3;
                }
            } else {
                if (yCompare <= 0) {
                    if (zCompare <= 0)
                        pos = 4;
                    else
                        pos = 5;
                } else {
                    if (zCompare <= 0)
                        pos = 6;
                    else
                        pos = 7;
                }
            }
            //Object o = this.children[pos].get(x,y,z);
            return this.children[pos].removeWithoutObject(x, y, z);
        }
    }


    public void update(Object x,Object y,Object z,Object newData) throws DBAppException {
        if(this.canInsert){
            for(int i=0;i<this.points.size();i++){
                Object x_=points.get(i).getX();
                Object y_=points.get(i).getY();
                Object z_=points.get(i).getZ();
                if(x_.equals(x) && y_.equals(y) && z_.equals(z))
                    this.points.get(i).setObject(newData);
            }
        }
        else{
            Object midx = getMid(topBoundary.getX(), BottomBoundary.getX());
            Object midy = getMid(topBoundary.getY(), BottomBoundary.getY());
            Object midz = getMid(topBoundary.getZ(), BottomBoundary.getZ());

            int xCompare = compareObject(x, midx);
            int yCompare = compareObject(y, midy);
            int zCompare = compareObject(z, midz);
            int pos;

            if(xCompare <= 0){
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos=0;
                    else
                        pos = 1;
                }else{
                    if(zCompare <= 0)
                        pos = 2;
                    else
                        pos = 3;
                }
            }else{
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos = 4;
                    else
                        pos = 5;
                }else {
                    if(zCompare <= 0)
                        pos = 6;
                    else
                        pos = 7;
                }
            }
            this.children[pos].update(x, y, z,newData);
        }
    }


    public Vector<Object> getX(Object x) throws DBAppException {
        if(this.canInsert){
            Vector<Object> vec=new Vector<>();
            for(int i=0;i<points.size();i++){
                if(points.get(i).getX().equals(x))
                    vec.add(points.get(i).getObject());
            }
            return vec;

        }

        Object midx = getMid(topBoundary.getX(), BottomBoundary.getX());
        int xCompare = compareObject(x, midx);
        int pos;
        Vector<Object> vec=new Vector<>();
        if(xCompare<=0){
            vec.add(children[0].getX(x));
            vec.add(children[1].getX(x));
            vec.add(children[2].getX(x));
            vec.add(children[3].getX(x));
        }
        else{
            vec.add(children[4].getX(x));
            vec.add(children[5].getX(x));
            vec.add(children[6].getX(x));
            vec.add(children[7].getX(x));
        }
        return vec;
    }



    public Vector<Object> getZ(Object z) throws DBAppException {
        if(this.canInsert){
            Vector<Object> vec=new Vector<>();
            for(int i=0;i<points.size();i++){
                if(points.get(i).getZ().equals(z))
                    vec.add(points.get(i).getObject());
            }
            return vec;

        }

        Object midZ = getMid(topBoundary.getZ(), BottomBoundary.getZ());
        int zCompare = compareObject(z, midZ);
        int pos;
        Vector<Object> vec=new Vector<>();
        if(zCompare<=0){
            vec.add(children[0].getZ(z));
            vec.add(children[2].getZ(z));
            vec.add(children[4].getZ(z));
            vec.add(children[6].getZ(z));
        }
        else{
            vec.add(children[1].getZ(z));
            vec.add(children[3].getZ(z));
            vec.add(children[5].getZ(z));
            vec.add(children[7].getZ(z));
        }
        return vec;
    }


    public Vector<Object> getY(Object y) throws DBAppException {
        if(this.canInsert){
            Vector<Object> vec=new Vector<>();
            for(int i=0;i<points.size();i++){
                if(points.get(i).getY().equals(y))
                    vec.add(points.get(i).getObject());
            }
            return vec;

        }

        Object midY = getMid(topBoundary.getY(), BottomBoundary.getY());
        int yCompare = compareObject(y, midY);
        int pos;
        Vector<Object> vec=new Vector<>();
        if(yCompare<=0){
            vec.add(children[0].getY(y));
            vec.add(children[1].getY(y));
            vec.add(children[4].getY(y));
            vec.add(children[5].getY(y));
        }
        else{
            vec.add(children[2].getY(y));
            vec.add(children[3].getY(y));
            vec.add(children[6].getY(y));
            vec.add(children[7].getY(y));
        }
        return vec;
    }




    public Vector<Object> getXY(Object x,Object y) throws DBAppException {
        if(this.canInsert){
            Vector<Object> vec=new Vector<>();
            for(int i=0;i<points.size();i++){
                if(points.get(i).getY().equals(y) && points.get(i).getX().equals(x))
                    vec.add(points.get(i).getObject());
            }
            return vec;

        }

        Object midY = getMid(topBoundary.getY(), BottomBoundary.getY());
        Object midX= getMid(topBoundary.getX(),BottomBoundary.getX());
        int yCompare = compareObject(y, midY);
        int xCompare = compareObject(x,midX);
        Vector<Object> vec=new Vector<>();
        if(xCompare<=0){
            if(yCompare<=0){
                vec.add(this.children[0].getXY(x,y));
                vec.add(this.children[1].getXY(x,y));
            }
            else{
                vec.add(this.children[2].getXY(x,y));
                vec.add(this.children[3].getXY(x,y));
            }
        }
        else{
            if(yCompare<=0){
                vec.add(this.children[4].getXY(x,y));
                vec.add(this.children[5].getXY(x,y));
            }
            else{
                vec.add(this.children[6].getXY(x,y));
                vec.add(this.children[7].getXY(x,y));
            }
        }
        return vec;
    }


    public Vector<Object> getXZ(Object x,Object z) throws DBAppException {
        if(this.canInsert){
            Vector<Object> vec=new Vector<>();
            for(int i=0;i<points.size();i++){
                if(points.get(i).getZ().equals(z) && points.get(i).getX().equals(x))
                    vec.add(points.get(i).getObject());
            }
            return vec;

        }

        Object midZ = getMid(topBoundary.getZ(), BottomBoundary.getZ());
        Object midX= getMid(topBoundary.getX(),BottomBoundary.getX());
        int zCompare = compareObject(z, midZ);
        int xCompare = compareObject(x,midX);
        Vector<Object> vec=new Vector<>();
        if(xCompare<=0){
            if(zCompare<=0){
                vec.add(this.children[0].getXZ(x,z));
                vec.add(this.children[2].getXZ(x,z));
            }
            else{
                vec.add(this.children[1].getXZ(x,z));
                vec.add(this.children[3].getXZ(x,z));
            }
        }
        else{
            if(zCompare<=0){
                vec.add(this.children[4].getXZ(x,z));
                vec.add(this.children[6].getXZ(x,z));
            }
            else{
                vec.add(this.children[5].getXZ(x,z));
                vec.add(this.children[7].getXZ(x,z));
            }
        }
        return vec;
    }

    public Vector<Object> HelperGitX(Object current,SQLTerm sqlTerm) throws DBAppException {
        String operator = sqlTerm._strOperator;
        switch (operator){
            case "=":
                return getX(current);
            case ">":
                if(this.canInsert){
                    Vector<Object> vec=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getX(),current)>0)
                            vec.add(points.get(i).getObject());
                    }
                    return vec;
                }
                Vector<Object> vec=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getX(),current)>0 || compareObject(this.children[i].BottomBoundary.getX(),current)>0)
                        vec.add(this.children[i].HelperGitX(current,sqlTerm));
                }
                return vec;
            case ">=":
                if(this.canInsert){
                    Vector<Object> vec2=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getX(),current)>=0)
                            vec2.add(points.get(i).getObject());
                    }
                    return vec2;
                }
                Vector<Object> vec2=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getX(),current)>=0 || compareObject(this.children[i].BottomBoundary.getX(),current)>=0)
                        vec2.add(this.children[i].HelperGitX(current,sqlTerm));
                }
                return vec2;
            case "<":
                if(this.canInsert){
                    Vector<Object> vec3=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getX(),current)<0)
                            vec3.add(points.get(i).getObject());
                    }
                    return vec3;
                }
                Vector<Object> vec3=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getX(),current)<0 || compareObject(this.children[i].BottomBoundary.getX(),current)<0)
                        vec3.add(this.children[i].HelperGitX(current,sqlTerm));
                }
                return vec3;
            case "<=":
                if(this.canInsert){
                    Vector<Object> vec4=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getX(),current)<=0)
                            vec4.add(points.get(i).getObject());
                    }
                    return vec4;
                }
                Vector<Object> vec4=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getX(),current)<=0 || compareObject(this.children[i].BottomBoundary.getX(),current)<=0)
                        vec4.add(this.children[i].HelperGitX(current,sqlTerm));
                }
                return vec4;
            case "!=":
                if(this.canInsert){
                    Vector<Object> vec5=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getX(),current)!=0)
                            vec5.add(points.get(i).getObject());
                    }
                    return vec5;
                }
                Vector<Object> vec5=new Vector<>();
                for(int i=0;i<8;i++){
                        vec5.add(this.children[i].HelperGitX(current,sqlTerm));
                }
                return vec5;
        }
        return new Vector<>();
    }

    public Vector<Object> HelperGitY(Object current,SQLTerm sqlTerm) throws DBAppException {
        String operator = sqlTerm._strOperator;
        switch (operator){
            case "=":
                return getY(current);
            case ">":
                if(this.canInsert){
                    Vector<Object> vec=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getY(),current)>0)
                            vec.add(points.get(i).getObject());
                    }
                    return vec;
                }
                Vector<Object> vec=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getY(),current)>0 || compareObject(this.children[i].BottomBoundary.getY(),current)>0)
                        vec.add(this.children[i].HelperGitY(current,sqlTerm));
                }
                return vec;
            case ">=":
                if(this.canInsert){
                    Vector<Object> vec2=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getY(),current)>=0)
                            vec2.add(points.get(i).getObject());
                    }
                    return vec2;
                }
                Vector<Object> vec2=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getY(),current)>=0 || compareObject(this.children[i].BottomBoundary.getY(),current)>=0)
                        vec2.add(this.children[i].HelperGitY(current,sqlTerm));
                }
                return vec2;
            case "<":
                if(this.canInsert){
                    Vector<Object> vec3=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getY(),current)<0)
                            vec3.add(points.get(i).getObject());
                    }
                    return vec3;
                }
                Vector<Object> vec3=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getY(),current)<0 || compareObject(this.children[i].BottomBoundary.getY(),current)<0)
                        vec3.add(this.children[i].HelperGitY(current,sqlTerm));
                }
                return vec3;
            case "<=":
                if(this.canInsert){
                    Vector<Object> vec4=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getY(),current)<=0)
                            vec4.add(points.get(i).getObject());
                    }
                    return vec4;
                }
                Vector<Object> vec4=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getY(),current)<=0 || compareObject(this.children[i].BottomBoundary.getY(),current)<=0)
                        vec4.add(this.children[i].HelperGitY(current,sqlTerm));
                }
                return vec4;
            case "!=":
                if(this.canInsert){
                    Vector<Object> vec5=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getY(),current)!=0)
                            vec5.add(points.get(i).getObject());
                    }
                    return vec5;
                }
                Vector<Object> vec5=new Vector<>();
                for(int i=0;i<8;i++){
                    vec5.add(this.children[i].HelperGitY(current,sqlTerm));
                }
                return vec5;
        }
        return new Vector<>();
    }

    public Vector<Object> HelperGitZ(Object current,SQLTerm sqlTerm) throws DBAppException {
        String operator = sqlTerm._strOperator;
        switch (operator){
            case "=":
                return getZ(current);
            case ">":
                if(this.canInsert){
                    Vector<Object> vec=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getZ(),current)>0)
                            vec.add(points.get(i).getObject());
                    }
                    return vec;
                }
                Vector<Object> vec=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getZ(),current)>0 || compareObject(this.children[i].BottomBoundary.getZ(),current)>0)
                        vec.add(this.children[i].HelperGitZ(current,sqlTerm));
                }
                return vec;
            case ">=":
                if(this.canInsert){
                    Vector<Object> vec2=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getZ(),current)>=0)
                            vec2.add(points.get(i).getObject());
                    }
                    return vec2;
                }
                Vector<Object> vec2=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getZ(),current)>=0 || compareObject(this.children[i].BottomBoundary.getZ(),current)>=0)
                        vec2.add(this.children[i].HelperGitZ(current,sqlTerm));
                }
                return vec2;
            case "<":
                if(this.canInsert){
                    Vector<Object> vec3=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getZ(),current)<0)
                            vec3.add(points.get(i).getObject());
                    }
                    return vec3;
                }
                Vector<Object> vec3=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getZ(),current)<0 || compareObject(this.children[i].BottomBoundary.getZ(),current)<0)
                        vec3.add(this.children[i].HelperGitZ(current,sqlTerm));
                }
                return vec3;
            case "<=":
                if(this.canInsert){
                    Vector<Object> vec4=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getZ(),current)<=0)
                            vec4.add(points.get(i).getObject());
                    }
                    return vec4;
                }
                Vector<Object> vec4=new Vector<>();
                for(int i=0;i<8;i++){
                    if(compareObject(this.children[i].topBoundary.getZ(),current)<=0 || compareObject(this.children[i].BottomBoundary.getZ(),current)<=0)
                        vec4.add(this.children[i].HelperGitZ(current,sqlTerm));
                }
                return vec4;
            case "!=":
                if(this.canInsert){
                    Vector<Object> vec5=new Vector<>();
                    for(int i=0;i<points.size();i++){
                        if(compareObject(points.get(i).getZ(),current)!=0)
                            vec5.add(points.get(i).getObject());
                    }
                    return vec5;
                }
                Vector<Object> vec5=new Vector<>();
                for(int i=0;i<8;i++){
                    vec5.add(this.children[i].HelperGitZ(current,sqlTerm));
                }
                return vec5;
        }
        return new Vector<>();
    }




    public Vector<Object> RangeOct(Object x, Object y, Object z, SQLTerm[] sqlTerms) throws DBAppException {
        Vector<Object> xVec = flattenArray(HelperGitX(x,sqlTerms[0]));
        Vector<Object> yVec = flattenArray(HelperGitY(y,sqlTerms[1]));
        Vector<Object> zVec = flattenArray(HelperGitZ(z,sqlTerms[2]));




        Vector<Object> res=new Vector<>();
        for(int i=0;i<xVec.size();i++){
            if(yVec.contains(xVec.get(i)))
                res.add(xVec.get(i));
        }
        for(int i=0;i<res.size();i++){
            if(!zVec.contains(res.get(i)))
                res.remove(res.get(i));
        }

        return res;
    }


    public Vector<Object> getYZ(Object y,Object z) throws DBAppException {
        if(this.canInsert){
            Vector<Object> vec=new Vector<>();
            for(int i=0;i<points.size();i++){
                if(points.get(i).getZ().equals(z) && points.get(i).getY().equals(y))
                    vec.add(points.get(i).getObject());
            }
            return vec;

        }

        Object midZ = getMid(topBoundary.getZ(), BottomBoundary.getZ());
        Object midY= getMid(topBoundary.getY(),BottomBoundary.getY());
        int zCompare = compareObject(z, midZ);
        int yCompare = compareObject(y,midY);
        Vector<Object> vec=new Vector<>();
        if(yCompare<=0){
            if(zCompare<=0){
                vec.add(this.children[0].getYZ(y,z));
                vec.add(this.children[4].getYZ(y,z));
            }
            else{
                vec.add(this.children[1].getYZ(y,z));
                vec.add(this.children[5].getYZ(y,z));
            }
        }
        else{
            if(zCompare<=0){
                vec.add(this.children[2].getYZ(y,z));
                vec.add(this.children[6].getYZ(y,z));
            }
            else{
                vec.add(this.children[3].getYZ(y,z));
                vec.add(this.children[7].getYZ(y,z));
            }
        }
        return vec;
    }


    public Object get(Object x,Object y,Object z) throws DBAppException {
        if(this.canInsert){
            for(int i=0;i<this.points.size();i++){
                Object x_=points.get(i).getX();
                Object y_=points.get(i).getY();
                Object z_=points.get(i).getZ();
                if(x_.equals(x) && y_.equals(y) && z_.equals(z))
                    return this.points.get(i).getObject();
            }
            return null;
        }
        else{
            Object midx = getMid(topBoundary.getX(), BottomBoundary.getX());
            Object midy = getMid(topBoundary.getY(), BottomBoundary.getY());
            Object midz = getMid(topBoundary.getZ(), BottomBoundary.getZ());

            int xCompare = compareObject(x, midx);
            int yCompare = compareObject(y, midy);
            int zCompare = compareObject(z, midz);
            int pos;

            if(xCompare <= 0){
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos=0;
                    else
                        pos = 1;
                }else{
                    if(zCompare <= 0)
                        pos = 2;
                    else
                        pos = 3;
                }
            }else{
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        pos = 4;
                    else
                        pos = 5;
                }else {
                    if(zCompare <= 0)
                        pos = 6;
                    else
                        pos = 7;
                }
            }
            return this.children[pos].get(x, y, z);
        }
    }





    public static Vector<Object> flattenArray(Vector<Object> arr) {
        Vector<Object> result = new Vector<Object>();
        for (Object obj : arr) {
            if (obj instanceof Vector<?>) {
                result.addAll(flattenArray((Vector<Object>) obj));
            } else {
                result.add(obj);
            }
        }
        return result;
    }
}
