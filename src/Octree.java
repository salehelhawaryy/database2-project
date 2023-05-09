import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.Date;
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
            return ((String) o1).compareTo(((String)o2));
        } else if(o1 instanceof Double) {
            return ((Double) o1).compareTo(((Double)o2));
        } else if(o1 instanceof Integer) {
            return ((Integer) o1).compareTo(((Integer)o2));
        } else {
            return ((Date) o1).compareTo(((Date)o2));
        }
    }

    public void split() throws DBAppException {
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

        for (int i = 0; i < this.points.size(); i++) {
            OctPoint current = points.get(i);
            int xCompare = compareObject(current.getX(), midx);
            int yCompare = compareObject(current.getY(), midy);
            int zCompare = compareObject(current.getZ(), midz);
            if(xCompare <= 0){
                if(yCompare <= 0){
                    if(zCompare <= 0)
                        children[0].insert();
                    else
                        pos = OctLocations.TopLeftBottom.getNumber();
                }else{
                    if(z <= midz)
                        pos = OctLocations.BottomLeftFront.getNumber();
                    else
                        pos = OctLocations.BottomLeftBack.getNumber();
                }
            }else{
                if(y <= midy){
                    if(z <= midz)
                        pos = OctLocations.TopRightFront.getNumber();
                    else
                        pos = OctLocations.TopRightBottom.getNumber();
                }else {
                    if(z <= midz)
                        pos = OctLocations.BottomRightFront.getNumber();
                    else
                        pos = OctLocations.BottomRightBack.getNumber();
                }
            }

        }
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
        if(points.size()<limit){
            points.add(insertion);
            return;
        }

        split();

        //if()

    }



}
