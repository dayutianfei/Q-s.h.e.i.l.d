package cn.dayutianfei.lusearch.server.loadmanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import org.apache.log4j.Logger;

import cn.dayutianfei.loadserver.client.cache.ClientCache;


public class LcLoadManager {

    private static Logger LOG = Logger.getLogger(LcLoadManager.class);
    private int maxThreadNumber = 0; // =<0表示无限大
    private ExecutorService assistantPool = null; //执行辅助线程池


    public LcLoadManager(int threadNumber){
        this.maxThreadNumber = threadNumber;
        if(this.maxThreadNumber>0){
            assistantPool = Executors.newFixedThreadPool(this.maxThreadNumber);
        }else{
            assistantPool = Executors.newCachedThreadPool();
        }
        LOG.info("the assistant for LcLoadManager is created with max threads" + this.maxThreadNumber);
    }
    
    public void load(String db, String tblName, List<String> data){
        Future<Object> re = assistantPool.submit(new LcLoadExecutor(db, tblName, data));
        try { 
            System.out.println(" "+re.get().toString());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args){
        ClientCache.updateCache();
        LcLoadManager manager = new LcLoadManager(4);
        String filePath = "/home/egret/win7/test_info.10w";
        List<List<String>> lines = new ArrayList<List<String>>();
        File file = new File(filePath);
        if (file.exists()) {
            if (!file.isDirectory()) {
                BufferedReader reader = null;
                String value = "";
                try {
                    reader = getBr(file);
                } catch (FileNotFoundException e) {
                    System.out.println("File '" + file.getName() + "' not frind !");
                }
                while (true) {
                    try {
                        value = reader.readLine();
                        if (value == null ) {
                            reader.close();
                            break;
                        }
//                      size += value.getBytes().length;
                        String[] values = value.split("\t", 30);
                        List<String> line = new ArrayList<String>();
                        for (String v : values) {
                            line.add(v);
                        }
//                      line = tjp.judge("default", "lineitem", line);
                      for(int i =0 ;i<100000;i++){
//                          size += value.getBytes().length;
                            lines.add(line);
                      }
                      break;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }else{
                System.out.println("path '" + filePath + "' is not a file ");
            }
        } else {
            System.out.println("path '" + filePath + "' is not exsits");
        }
        System.out.println("done loading the data");
        long start = System.currentTimeMillis();
        for(List<String> data : lines){
            manager.load("test", "test_info", data);
        }
        System.out.println("total time "+ (System.currentTimeMillis()-start));
    
    }
    private static BufferedReader getBr(File file) throws FileNotFoundException {
        FileInputStream fis1 = new FileInputStream(file);
        InputStreamReader reader1 = new InputStreamReader(fis1);
        BufferedReader br1 = new BufferedReader(reader1);
        return br1;
    }
}
