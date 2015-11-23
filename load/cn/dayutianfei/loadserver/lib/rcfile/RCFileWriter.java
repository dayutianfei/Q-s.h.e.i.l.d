package cn.dayutianfei.loadserver.lib.rcfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: RCFileWriter
 * @Description:RCFile写入类
 * @author: hzb
 * @date: 2015年6月25日 下午3:42:48
 * 
 */
public class RCFileWriter {
    private final static Logger LOG = LoggerFactory.getLogger(RCFileWriter.class);
    private RCFile.Writer writer = null;
    private FileSystem fs;


    /**
     * @throws IOException
     * @Title: RCFileWriter
     * @Description: 初始化RCFileWriter
     * @param: @param table
     * @param: @param path
     * @param: @param conf
     * @throws
     */
    public RCFileWriter(Path path, Configuration conf, int columsNumber,
            String compressClass, int replicationNumber
            ) throws IOException {
        // TODO Auto-generated constructor stub
        conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columsNumber);// 列数
        conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, 4 * 1024 * 1024);// 决定行数参数一
        fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            conf.setBoolean("isExist", true);
        }
        else {
            conf.setBoolean("isExist",false);
        }
        // writer = new RCFile.Writer(fs, conf, path);
        CompressionCodec codec = null;
        if (null != compressClass && !"NONE".equalsIgnoreCase(compressClass)) {
            try {
                Class<?> c = Class.forName(compressClass);
                codec = (CompressionCodec) c.newInstance();
            }
            catch (Exception e) {
                LOG.error("Can not load class:" + compressClass);
                codec = null;
            }
        }
        writer = new RCFile.Writer(fs, conf, path, (short) replicationNumber, codec);
    }


    /**
     *  * @Title: append  * @Description: 写入一条数据  * @param: @param records     *
     * @return: void     * @throws  
     */
    public boolean append(String[] records) {
        if (writer == null) {
            return false;
        }
        BytesRefArrayWritable cols = new BytesRefArrayWritable(records.length);
        BytesRefWritable col = null;
        int count = 0;
        for (String record : records) {
            col = new BytesRefWritable(Bytes.toBytes(record), 0, Bytes.toBytes(record).length);
            cols.set(count, col);
            count++;
        }
        try {
            writer.append(cols);
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }


    /**
     *  * @Title: flush  * @Description: flush 将缓存中的数据写入RCFile  * @param:      *
     * @return: void     * @throws  
     */
    public void flush() {

    }


    /**
     *  * @Title: close  * @Description: 关闭文件  * @param:      * @return: void   
     *  * @throws  
     */
    public void close() {
        if (writer != null) {
            try {
                writer.close();
            }
            catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
