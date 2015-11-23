package cn.dayutianfei.loadserver.prototype;

import java.util.ArrayList;

import org.apache.avro.Schema;

public class TableInfo {
    public static String COMPRESS="_compress";
    public static String REPLICATION="_replication";
    private String dbName;//数据库名
    private String dbUUID;
    private String tableName;//表名
    private String tableUUID;
    private ArrayList<Field> fields = new ArrayList<Field>();//字段
//  private ArrayList<Field> oldFields;//旧的字段
    private ArrayList<Partition> partitions = new ArrayList<Partition>();//分区
    private int fieldCount;//字段数
    private int _fieldTotalCount;//写rcFile的字段数
    private int dataLifeTime;//生命周期时间
    private String location;//hdfs存储路径
    private String compress = "NONE";//压缩方式
    private int replication = 1;//副本数
    
    public TableInfo(){  }

    public String getDbName() {
        return dbName;
    }
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public ArrayList<Field> getFields() {
        return fields;
    }
    public void setFields(ArrayList<Field> fields) {
        this.fields = fields;
        this._fieldTotalCount = fields.size();
    }
    public int getFieldCount() {
        return fieldCount;
    }
    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }
    public int getDataLifeTime() {
        return dataLifeTime;
    }
    public void setDataLifeTime(int dataLifeTime) {
        this.dataLifeTime = dataLifeTime;
    }
    
    public String getLocation() {
        return location;
    }
    public void setLocation(String location) {
        this.location = location;
    }
    
    public ArrayList<Partition> getPartitions() {
        return partitions;
    }

    public void setPartitionInfos(ArrayList<Partition> partitions) {
        this.partitions = partitions;
    }
    
    public int get_fieldTotalCount() {
        return _fieldTotalCount;
    }

    public void set_fieldTotalCount(int _fieldTotalCount) {
        this._fieldTotalCount = _fieldTotalCount;
    }

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }

    public int getReplication() {
        return replication;
    }

    public void setReplication(int replication) {
        this.replication = replication;
    }

    public Schema getArraySchema() {
        // TODO Auto-generated method stub
        return null;
    }
    
    public Field getFieldByCode(String fieldCode) {
        for (Field field : fields) {
            String fn = field.getFieldIndex()+"";
            if (fn.equals(fieldCode)) {
                return field;
            }
        }
        return null;
    }
    
    public String getDbUUID() {
        return dbUUID;
    }

    public void setDbUUID(String dbUUID) {
        this.dbUUID = dbUUID;
    }

    public String getTableUUID() {
        return tableUUID;
    }

    public void setTableUUID(String tableUUID) {
        this.tableUUID = tableUUID;
    }
    
    public String toSchema(){
        StringBuilder sb = new StringBuilder();
        sb.append("{\"namespace\": \"cn.ac.iie.fstore.file.avro\",\"type\": \"record\",\"name\":");
        sb.append("\""+tableName+"\",\"fields\": [");
        for(int i = 0 ; i< fields.size();i++){
            Field col = fields.get(i);
            String colname = col.getFieldName();
            String coltype = col.getType().toString();
            if(coltype.equalsIgnoreCase("binary")){
                coltype = "bytes";
            }else{
                coltype = "string";
            }
            if(i==0){
                sb.append("{\"name\": \""+colname+"\", \"type\": \""+coltype+"\"}");
            }else{
                sb.append(",{\"name\": \""+colname+"\", \"type\": \""+coltype+"\"}");
            }
        }
        sb.append("]}");
        return sb.toString(); 
    }

    public String toString(){
        return "TableInfo[db="+dbName+",dbUUID="+dbUUID+
                ",table="+tableName+",tableUUID="+tableUUID+
                ",Location="+location+",Fields:"+fields+
                ",Partitions="+partitions+"]";
    }
        
}
