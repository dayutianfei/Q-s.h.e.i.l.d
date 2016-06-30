package cn.dayutianfei.hdfs.parquet;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class ReadMain {
    public static void main(String[] args){
        try {
            QParquetFileReader reader = new QParquetFileReader(new Path("file:///temp/hawq/xyx_appendx"));
            boolean c = true;
            while(c){
                try{
                    String[] t = reader.read();
                    if(null == t){
                        c = false;
                        break;
                    }
                    System.out.println(printstr(t));
                }catch(Exception e){
                    c = false;
                    e.printStackTrace();
                }
            }
        }
        catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static String printstr(String[] input){
        if(null == input || input.length == 0){
            return null;
        }else{
            String out = "";
            for(String temp : input){
                out+="\t" + temp;
            }
            return out;
        }
    }
}
