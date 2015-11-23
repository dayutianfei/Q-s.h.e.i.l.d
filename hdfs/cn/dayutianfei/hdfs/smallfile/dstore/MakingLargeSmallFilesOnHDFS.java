package cn.dayutianfei.hdfs.smallfile.dstore;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * totalFileNumber is 100000 
 * totalFileSizeInBytes is 26288566954 
 * fileAverageSizeInBytes is 262885 
 * speed is 10.771078979374815 MB/s
 *
 */
public class MakingLargeSmallFilesOnHDFS {
	private static final Log LOG = LogFactory.getLog("FileKey");
	static Configuration conf = new Configuration();
    static FileSystem hdfs;
    static {
        try {
        	conf.set("fs.defaultFS", "hdfs://172.16.8.34:8020");
        	conf.setInt("dfs.replication", 1);
            hdfs = FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String hdfsRootURI = "/smallfile/dstore/"+(System.currentTimeMillis()/1000); //数据在HDFS的根目录
		String currentFileNo = "/smallfile/dstore/"+"version"; // 存储当前最新文件编号
		LOG.info("hdfs root uri is "+hdfsRootURI);
		
		long fileTargetNumber = 100000; //每个子目录生成的小文件数量
		int fileTargetAverageSizeInBytes = 512 * 1024; //每个小文件的最大值
		long fileMaxSize = 2 * 1024 * 1024 * 1024l ;//每个大文件的最大值
		Random r = new Random();
		
		long totalFileNumber = 0; //已经生成的文件总数
		long currentLargeFileSizeInBytes = 0 ;
		long totalFileSizeInBytes = 0; //已经生成的文件总量（byte）
		long fileAverageSizeInBytes = 0; //已经生成的文件的平均大小
		
		String currentFileVersion = null;
		String largeFile = null;
		long pos = 0;
		FSDataOutputStream output = null;
		long lasttime =0 ;
		for(int i = 0 ; i < fileTargetNumber; i ++){
			long start = System.currentTimeMillis();
			if(i==0 || currentLargeFileSizeInBytes >= fileMaxSize){
				if(i!=0){
					output.close();
					pos = 0;
					currentLargeFileSizeInBytes=0;
				}
				currentFileVersion = FileRule.newFileVersion(hdfs, currentFileNo);
				largeFile = hdfsRootURI + "/" +currentFileVersion;
				Path dst = new Path(largeFile);
				output = hdfs.create(dst);
				LOG.info("current file version: "+currentFileVersion);
				LOG.info("hdfs large small file on " + dst.toString());
			}
			int fileWeight = r.nextInt(fileTargetAverageSizeInBytes);
			byte[] nbyte = new byte[fileWeight];
		    r.nextBytes(nbyte);
		    output.write(nbyte);
		    totalFileNumber++;
		    currentLargeFileSizeInBytes+=fileWeight;
		    totalFileSizeInBytes+=fileWeight;
		    LOG.info(largeFile+"-"+pos+"-"+fileWeight);
		    pos+=fileWeight;
		    lasttime += (System.currentTimeMillis() - start);
		}
		output.close();
		fileAverageSizeInBytes = totalFileSizeInBytes/totalFileNumber;
		LOG.info("totalFileNumber is "+totalFileNumber);
		LOG.info("totalFileSizeInBytes is "+totalFileSizeInBytes);
		LOG.info("fileAverageSizeInBytes is "+fileAverageSizeInBytes);
		LOG.info("speed is "+totalFileSizeInBytes/1024.0/1024.0/lasttime*1000 + " MB/s");
	}
	
	public static void createFile(String fileName, byte[] fileContent) throws IOException {
		long begin = System.currentTimeMillis();
        Path dst = new Path(fileName);
        byte[] bytes = fileContent;
        FSDataOutputStream output = hdfs.create(dst);
        output.write(bytes);
        output.flush();
        output.close();
        LOG.info("file "+ fileName + " created on hdfs cost: "+(System.currentTimeMillis()-begin) + " ms");
    }
}
