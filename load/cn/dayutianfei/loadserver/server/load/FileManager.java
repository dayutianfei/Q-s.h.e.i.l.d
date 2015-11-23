package cn.dayutianfei.loadserver.server.load;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.lib.rcfile.RCFileWriter;

public class FileManager {
    
    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);
    
    public static final long maxRecordNumInFile = 1024 * 1024 * 1024 * 1;
    private ConcurrentHashMap<String, RCFileWriter> writerCache = new ConcurrentHashMap<String, RCFileWriter>();
    private Configuration conf = new Configuration();;
    private final String rootPath = "/wzy/rcfile";
    
    public synchronized RCFileWriter getWriter(String key){
        if(!writerCache.contains(key)){
            Path path = new Path(rootPath+"/"+key+"/"+System.nanoTime());
            try {
                writerCache.putIfAbsent(key, new RCFileWriter(path, conf, 20, "NONE", 1));
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return writerCache.get(key);
    }
    
    public synchronized void close(){
        if(!writerCache.isEmpty()){
            for(String key: writerCache.keySet()){
                writerCache.get(key).close();
                writerCache.remove(key);
            }
        }
        LOG.info("close all writers");
    }
}
