package cn.dayutianfei.hdfs.smallfile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * 测试结果：
 * 1. 远程读取（单线程）：(非首次运行结果，HDFS可能有缓存)
 * 
 * * 从10w个文件中读取：
 *   totalFileNumber is 10000 
 *   totalFileSizeInBytes is 2423751843 
 *   totalFileReadCost is 467951 ms
 *  -----4.93MB/s, 21.36个/s
 *
 */
public class ReadingManySmallFilesFromHDFS {
	private static final Log LOG = LogFactory.getLog(ReadingManySmallFilesFromHDFS.class);
	static Configuration conf = new Configuration();
    static FileSystem hdfs;
    static {
        try {
        	conf.set("fs.defaultFS", "hdfs://172.16.8.34:8020");
            hdfs = FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String hdfsRootURI = "/test/1436447078"; //数据在HDFS的根目录
		LOG.info("hdfs root uri is "+hdfsRootURI);
		int file2ReadNumber = 10000; //目标读取的文件数量
		Random r = new Random();
		
		long totalFileNumber = 0; //已经读取的文件总数
		long totalFileSizeInBytes = 0; //已经读取的文件总量（byte）
		long totalFileReadTimeInMs = 0; //读取文件的耗时
		
		for(int i = 0 ; i < file2ReadNumber; i ++){
			int dir = r.nextInt(10);
			int fileName = r.nextInt(10000);
			String file = hdfsRootURI + "/" + dir + "/file-" + fileName;
			long filesize = 0;
			long start = System.currentTimeMillis();
			try {
				filesize=readFile(file);
			} catch (IOException e) {
				e.printStackTrace();
			}
			long cost = System.currentTimeMillis()-start;
			LOG.info("file "+file+ " size:"+filesize+" read from hdfs cost "+cost+" ms");
			totalFileReadTimeInMs+=cost;
			 totalFileNumber++;
			 totalFileSizeInBytes+=filesize;
		}
		LOG.info("totalFileNumber is "+totalFileNumber);
		LOG.info("totalFileSizeInBytes is "+totalFileSizeInBytes);
		LOG.info("totalFileReadCost is "+totalFileReadTimeInMs + " ms");
	}
	
	public static void readFromLargeFile(String fileName, byte[] fileContent, long pos, int length) throws IOException {
		long begin = System.currentTimeMillis();
        Path dst = new Path(fileName);
        byte[] bytes = fileContent;
        FSDataInputStream input = hdfs.open(dst);
        input.read(pos, bytes, 0, length);
        input.close();
        LOG.info("file "+ fileName + " read "+length+" bytes on hdfs cost: "+(System.currentTimeMillis()-begin) + " ms");
    }
	public static long readFile(String fileName) throws IOException {
		long begin = System.currentTimeMillis();
        Path dst = new Path(fileName);
        FSDataInputStream input = hdfs.open(dst);
        BufferedReader bis = new BufferedReader(new InputStreamReader(input,"utf-8"));     
        String temp;
        StringBuilder content = new StringBuilder();
        while ((temp = bis.readLine()) != null) {
        	content.append(temp);
        }         
        bis.close();  
        LOG.info("file "+ fileName + " read "+content.length()+" bytes on hdfs cost: "+(System.currentTimeMillis()-begin) + " ms");
        return content.length();
    }
}
