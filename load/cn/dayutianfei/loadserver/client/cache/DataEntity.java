package cn.dayutianfei.loadserver.client.cache;

import java.util.List;

/**
 * 事件(Event)就是通过 Disruptor 进行交换的数据类型。
 * @author wzy
 */
public class DataEntity{
    private String db;
    private String tblName;
    private String partName; //TODO：该字段视测试情况删除
    private String opera;
    private List<String> operators;
    private List<Object> data;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTblName() {
        return tblName;
    }

    public void setTblName(String tblName) {
        this.tblName = tblName;
    }

    public String getPartName() {
        return partName;
    }

    public void setPartName(String partName) {
        this.partName = partName;
    }

    public String getOpera() {
        return opera;
    }

    public void setOpera(String opera) {
        this.opera = opera;
    }

    public List<String> getOperators() {
        return operators;
    }

    public void setOperators(List<String> operators) {
        this.operators = operators;
    }

    public List<Object> getData() {
        return data;
    }

    public void setData(List<Object> data) {
        this.data = data;
    }
}
