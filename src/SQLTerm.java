public class SQLTerm implements Comparable{
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;



    public SQLTerm(){

    }

    public SQLTerm(String _strTableName,String _strColumnName,String _strOperator,Object _objValue){
        this._strTableName=_strTableName;
        this._strColumnName=_strColumnName;
        this._strOperator=_strOperator;
        this._objValue=_objValue;
    }

    @Override
    public int compareTo(Object o) {
        SQLTerm compare=(SQLTerm) o;
            if(this._strTableName.compareTo(compare._strTableName)>0)
                return 1;
            else if(this._strTableName.compareTo(compare._strTableName)<0)
                return -1;
            return 0;
        }

}
