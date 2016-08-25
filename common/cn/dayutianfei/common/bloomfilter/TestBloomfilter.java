package cn.dayutianfei.common.bloomfilter;

import ict.BloomFilter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 该类主要实现节点上的Bloomfilter缓存信息
 * @author wzy
 */

public class TestBloomfilter {
	
	public static String bloomfilterPath = "/tmp/bf";
	//节点中存储的bloomfilter信息，其中key:唯一标识, BloomFilter<String>:对应的Bloomfilter
	public static Map<String, BloomFilter<String>> bloomfilters = new ConcurrentHashMap<String, BloomFilter<String>>();
	//节点中缓存的key和其加入缓存的时间, 到秒级
	public static Map<String, Long> bfInTheCache = new ConcurrentHashMap<String, Long>();
	public static int maxBitSize = 500 * 10000;
	
	public static void update(String mark, String singleKey){
		synchronized (bloomfilters) {
			if (!bloomfilters.containsKey(mark)) {
				bloomfilters.put(mark, new BloomFilter<String>(maxBitSize,maxBitSize));
			}
		}
		bloomfilters.get(mark).add(singleKey);
		synchronized (bfInTheCache) {
			bfInTheCache.put(mark, System.currentTimeMillis() );
		}
	}
	
	public static boolean isContainShardkey(String mark, String key){
		if (!bloomfilters.containsKey(mark)) {
			return false;
		}
		if (bloomfilters.get(mark).contains(key)) {
			return true;
		}else{
		    return false;
		}
	}
	
	public static boolean readFromDisk(String path){
		File file = new File(path);
		if (file.exists() && !file.isFile()) {
			File[] files = file.listFiles();
			for (File singleFile: files){
				readBloomfilterForMark(path, singleFile.getName());
			}
		} else {
			return false;
		}
		return true;
	}
	
	public static boolean removeSingleBloomfilterFromDisk(String path){
		File file = new File(path);
		if (file.exists() && file.isFile()) {
			file.delete();
		} else {
			return false;
		}
		return true;
	}
	
	@SuppressWarnings("unchecked")
	public static boolean readBloomfilterForMark(String path, String mark){
		File file = new File(path);
		if (file.exists() && file.isFile()) {
		    BloomFilter<String> bloomfilter = new BloomFilter<String>(maxBitSize,maxBitSize);
			FileInputStream in = null;
			ObjectInputStream objectInputStream = null;
			try {
				in = new FileInputStream(path);
				objectInputStream = new ObjectInputStream(in);  
				System.out.println("the bloomfilter is formed to markName : " + mark + " on "+ path);
				bloomfilter = (BloomFilter<String>)objectInputStream.readObject();
			} catch (FileNotFoundException e) {
			    System.out.println(e.getMessage());
				return false;
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				return false;
			} finally{
				try {
					in.close();
					objectInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			//如果从文件中取出的数据放入到bf中，不为空，则认为是正确。
			if (null == bloomfilter){
				return false;
			}
			synchronized(bloomfilters){
                bloomfilters.put(mark, bloomfilter);
            }
		} else {
			System.out.println("path is not a right path");
			return false;
		}
		return true;
	}
	
	public static void saveCachedBloomfiltersToDisk() throws IOException{
		for (String mark : bloomfilters.keySet()){
			String temPath = bloomfilterPath;
			BloomFilter<String> bloomfilter = bloomfilters.get(mark);
			File file = new File(bloomfilterPath);
			if (!file.exists()) {
				file.mkdirs();  
			}
			if(bloomfilterPath.endsWith("/")){
				temPath = temPath + mark;
			}else{
				temPath = temPath + "/" + mark;
			}
			System.out.println(temPath);
			File file_ = new File(temPath);
			if(file_.exists()){
			    file_.delete();
			}
			FileOutputStream outStream = new FileOutputStream(file_);  
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);  
            objectOutputStream.writeObject(bloomfilter);
            objectOutputStream.flush();
            objectOutputStream.close();
            outStream.flush();
            outStream.close();  
		}
	}
	
	/***
	 * 测试
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		//存入文件再读取之后
		String singleKey = "1";
		String mark = "test";
		System.out.println("key " + singleKey  + " : "+ isContainShardkey(mark, singleKey));
		update(mark,singleKey);
		System.out.println("key " + singleKey  + " : "+ isContainShardkey(mark, singleKey));
		saveCachedBloomfiltersToDisk();
		readBloomfilterForMark("/tmp/bf/test", "test");
		System.out.println("key " + singleKey  + " : "+ isContainShardkey(mark, singleKey));
		System.out.println("key " + singleKey  + " : "+ isContainShardkey(mark, "notExists"));
	}
}
