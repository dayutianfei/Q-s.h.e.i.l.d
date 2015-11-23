package cn.dayutianfei.hdfs.smallfile;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 测试结果：
 * 1. 远程读取（单线程）：(非首次运行结果，HDFS可能有缓存)
 * 
 * * 从1个文件中读取：512K以下
 *   totalFileNumber is 10000 
 *   totalFileSizeInBytes is 2635800170 
 *   totalFileReadCost is 305273 ms
 *  -----8.23MB/s, 32.75个/s
 *  
 * * 从5个文件中读取：512K以下
 *   totalFileNumber is 10000 
 *   totalFileSizeInBytes is 2603797984 
 *   totalFileReadCost is 356955 ms
 *  -----6.96MB/s, 28.01个/s
 *  
 * * 从10个文件中读取：512K以下
 *  	totalFileNumber is 10000 
 *  	totalFileSizeInBytes is 2617100205 
 *  	totalFileReadCost is 384115 ms
 *  -----6.49MB/s, 26.03个/s
 *  
 * * 从10个文件中读取：1MB以下
 *  	totalFileNumber is 10000 
 *  	totalFileSizeInBytes is 5234311406 
 *  	totalFileReadCost is 628438 ms
 *  -----7.94MB/s, 15.90个/s
 *  
 * * 从10个文件中读取：1MB以下
 *  	totalFileNumber is 10000 
 *  	totalFileSizeInBytes is 5266843822 
 *  	totalFileReadCost is 574637 ms
 *  -----8.74MB/s, 17.40个/s
 */

public class ReadingLargeSmallFilesFromHDFS {
	private static final Log LOG = LogFactory.getLog(ReadingLargeSmallFilesFromHDFS.class);
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
		String hdfsRootURI = "/test/1436448176"; //数据在HDFS的根目录
		LOG.info("hdfs root uri is "+hdfsRootURI);
		int file2ReadNumber = 10000; //目标读取的文件数量
		int fileTargetAverageSizeInBytes = 1024 * 1024; //平均每个文件的大小
		Random r = new Random();
		
		long totalFileNumber = 0; //已经读取的文件总数
		long totalFileSizeInBytes = 0; //已经读取的文件总量（byte）
		long totalFileReadTimeInMs = 0; //读取文件的耗时
		
		for(int i = 0 ; i < file2ReadNumber; i ++){
			int dir = r.nextInt(10);
			String file = hdfsRootURI + "/" + dir + "/" + dir;
			int filesize = r.nextInt(fileTargetAverageSizeInBytes);
			byte[] nbyte = new byte[filesize];
			long pos = Math.abs(r.nextLong()%(1024*1024*1024*2));
			long start = System.currentTimeMillis();
			try {
				readFromLargeFile(file,nbyte,pos,filesize);
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
        //input.close();
        LOG.info("file "+ fileName + " read "+length+" bytes on hdfs cost: "+(System.currentTimeMillis()-begin) + " ms");
    }
	public static void readFile(String fileName, byte[] fileContent) throws IOException {
		long begin = System.currentTimeMillis();
        Path dst = new Path(fileName);
        byte[] bytes = fileContent;
        FSDataInputStream input = hdfs.open(dst);
        input.read(bytes);
        input.close();
        LOG.info("file "+ fileName + " read "+bytes.length+" bytes on hdfs cost: "+(System.currentTimeMillis()-begin) + " ms");
    }
}
