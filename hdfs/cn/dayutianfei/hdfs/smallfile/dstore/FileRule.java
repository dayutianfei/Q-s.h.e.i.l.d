package cn.dayutianfei.hdfs.smallfile.dstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

public class FileRule {

	private static final Log LOG = LogFactory.getLog("FileKey");
	public static long FILE_SIZE = 1 * 1024 * 1024 * 1024l ;// 单个文件理论大小
	public static String[] versionCandidate = new String[]
			{"0","1","2","3","4","5","6","7","8","9",
			"a","b","c","d","e","f","g","h","i","j",
			"k","l","m","n","o","p","q","r","s","t",
			"u","v","w","x","y","z",
			"A","B","C","D","E","F","G","H","I","J",
			"K","L","M","N","O","P","Q","R","S","T",
			"U","V","W","X","Y","Z"};
	public static long versions = 0l;
	/**
	 *  文件的名字使用数字、字母组成，区分大小写，长度为6
	 *  理论最大文件数量：56800235584
	 * @param args
	 * @throws Exception 
	 */
	public static String newFileVersion(FileSystem hdfs, String fileVersionPath) throws Exception {
		long now = System.currentTimeMillis()/1000;
		String out = now + "-" +number2Version(versions++);
		return out;
		
		//待测试
		/*
		String version = "";
		String currentNumber = "0";
		int retryMax = 1000;
		while(true){
			try{
				//获取当前版本号
				Path dst = new Path(fileVersionPath);
		        FSDataInputStream input = hdfs.open(dst);
		        BufferedReader bis = new BufferedReader(new InputStreamReader(input,"utf-8"));     
		        String temp;
		        while ((temp = bis.readLine()) != null) {
		        	currentNumber = temp;
		        	break;
		        }         
		        bis.close();
		        //删除版本存储文件
		        hdfs.deleteOnExit(dst);
		        //创建新文件，写入最新的版本号
		        Path new_dst = new Path(fileVersionPath);
		        byte[] bytes = Long.toString(Long.parseLong(currentNumber)+1).getBytes();
		        FSDataOutputStream output = hdfs.create(new_dst);
		        output.write(bytes);
		        output.flush();
		        output.close();
		        break;
			}catch(Exception e){
				//上述过程如有错误，需要重试
				if(retryMax<0){
					throw new Exception("Max error retry time out!");
				}
				retryMax--;
				Thread.sleep(100);
				//当版本文件不存在时，写入一个新文件
				try{
					Path new_dst = new Path(fileVersionPath);
			        byte[] bytes = Long.toString(Long.parseLong(currentNumber)+1).getBytes();
			        FSDataOutputStream output = hdfs.create(new_dst);
			        output.write(bytes);
			        output.flush();
			        output.close();
			        break;
				}catch(Exception ee){
					continue;
				}
			}
		}
        version = number2Version(Long.parseLong(currentNumber));
		return version;
		*/
	}
	
	/**
	 * 版本号：使用数字、字母组成，区分大小写，长度为6
	 * @param number
	 * @return
	 */
	public static String number2Version(long number){
		LOG.info("number input is "+number);
		String version = "";
		String[] _version = new String[]{"0","1","2","3","4","5"};
		long _number = number;
		for(int i = 0; i < 6; i++){
			int pos = (int)_number % versionCandidate.length;
			_version[5-i]=versionCandidate[pos];
			_number = _number / versionCandidate.length;
		}
		for(String temp : _version){
			version+=temp;
		}
		LOG.info("version output is "+version);
		return version;
	}

	public static void main(String[] args){
		System.out.println(FileRule.number2Version(0));
		System.out.println(FileRule.number2Version(56));
		System.out.println(FileRule.number2Version(128));
		System.out.println(FileRule.number2Version(7234234));
	}
}
