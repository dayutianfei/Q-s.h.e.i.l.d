package cn.dayutianfei.hdfs.sequencefile;

import java.io.IOException;
import java.net.URI;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
 
public class SequenceFileWriteDemo {
    private static final String[] DATA = { "One, two, buckle my shoe",
            "Three, four, shut the door", "Five, six, pick up sticks",
            "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };
 
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://xxx.xxx.xxx.xx:9000");
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            String compressType = args[1];
            System.out.println("compressType "+compressType);
             
                //  Writer : Uncompressed records. 
            if(compressType.equals("1") ){
                System.out.println("compress none");
                writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),value.getClass(),CompressionType.NONE);
            }else if(compressType .equals("2") ){
                System.out.println("compress record");
                //RecordCompressWriter : Record-compressed files, only compress values. 
                writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),value.getClass(),CompressionType.RECORD);   
            }else if(compressType.equals("3") ){
                System.out.println("compress block");
                //  BlockCompressWriter : Block-compressed files, both keys & values are collected in 'blocks' separately and compressed. The size of the 'block' is configurable. 
                writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),value.getClass(),CompressionType.BLOCK);
            }
             
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key,value);
                writer.append(key, value);
                 
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}