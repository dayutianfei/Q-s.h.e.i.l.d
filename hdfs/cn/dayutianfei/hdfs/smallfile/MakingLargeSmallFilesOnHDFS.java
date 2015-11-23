package cn.dayutianfei.hdfs.smallfile;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class MakingLargeSmallFilesOnHDFS {
	private static final Log LOG = LogFactory.getLog(MakingLargeSmallFilesOnHDFS.class);
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
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String hdfsRootURI = "/test/"+(System.currentTimeMillis()/1000); //数据在HDFS的根目录
		LOG.info("hdfs root uri is "+hdfsRootURI);
		int subDirectoryNumber = 10; //数据存储的子目录的数量
		long fileNumberInEachDir = 10000; //每个子目录生成的小文件数量
		int fileTargetAverageSizeInBytes = 512 * 1024; //平均每个文件的大小
		Random r = new Random();
		
		long totalFileNumber = 0; //已经生成的文件总数
		long totalFileSizeInBytes = 0; //已经生成的文件总量（byte）
		long fileAverageSizeInBytes = 0; //已经生成的文件的平均大小
		
		for(int i = 0 ; i < subDirectoryNumber; i ++){
			String subDirPath = hdfsRootURI + "/" + i + "/";
			LOG.info("hdfs sub directory uri is "+subDirPath);
			Path dst = new Path(subDirPath+i);
			LOG.info("hdfs large small file on " + dst.toString());
			FSDataOutputStream output = hdfs.create(dst);
			long pos = 0;
			long fileNumber = 0;
			for(long j = 0 ; j< fileNumberInEachDir; j++){
				int fileWeight = r.nextInt(fileTargetAverageSizeInBytes);
				byte[] nbyte = new byte[fileWeight];
			    r.nextBytes(nbyte);
			    output.write(nbyte);
			    totalFileNumber++;
			    totalFileSizeInBytes+=fileWeight;
			    LOG.info(j+" "+pos+" "+fileWeight);
			    pos+=fileWeight;
			    fileNumber++;
			}
			output.close();
			LOG.info("hdfs large small file on " + dst.toString() + " size is : "+ pos +", having "+fileNumber+" files in it");
		}
		fileAverageSizeInBytes = totalFileSizeInBytes/totalFileNumber;
		LOG.info("totalFileNumber is "+totalFileNumber);
		LOG.info("totalFileSizeInBytes is "+totalFileSizeInBytes);
		LOG.info("fileAverageSizeInBytes is "+fileAverageSizeInBytes);
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
