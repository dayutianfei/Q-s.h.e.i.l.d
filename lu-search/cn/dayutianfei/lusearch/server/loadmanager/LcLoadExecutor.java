package cn.dayutianfei.lusearch.server.loadmanager;

import java.util.List;
import java.util.concurrent.Callable;

public class LcLoadExecutor implements Callable<Object>{

    private String db;
    private String tbl;
    private List<String> data;
    private TypeJudgePolicy judger = new TypeJudgePolicy();
    
    public LcLoadExecutor(String db, String tbl, List<String> data){
        this.setData(data);
        this.setDb(db);
        this.setTbl(tbl);
    }
    @Override
    public Object call() throws Exception {
        judger.judge(this.getDb(), this.getTbl(), this.getData());
        String re = Thread.currentThread().getId() + "success";
        return re;
    }
    public String getDb() {
        return db;
    }
    public void setDb(String db) {
        this.db = db;
    }
    public String getTbl() {
        return tbl;
    }
    public void setTbl(String tbl) {
        this.tbl = tbl;
    }
    public List<String> getData() {
        return data;
    }
    public void setData(List<String> data) {
        this.data = data;
    }
}
