package cn.dayutianfei.hadoop.ha;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HAClientConnection {
    public static void main(String[] args){
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://nameservice1");
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        conf.set("dfs.nameservices", "nameservice1");
//        conf.set("dfs.ha.namenodes.nameservice1", "namenode25,namenode91");
//        conf.set("dfs.namenode.rpc-address.nameservice1.namenode25", "mdss62:8020");
//        conf.set("dfs.namenode.rpc-address.nameservice1.namenode91", "mdss63:8020");
//        conf.set("dfs.client.failover.proxy.provider.nameservice1",
//            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        FileSystem fs = null;
        try {
           fs = FileSystem.get(conf);
           FileStatus[] list = fs.listStatus(new Path("/"));
           for (FileStatus file : list) {
             System.out.println(file.getPath().getName());
            }
        } catch (IOException e) {
           e.printStackTrace();
        } finally{
            try {
              fs.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
        }
    }
}
