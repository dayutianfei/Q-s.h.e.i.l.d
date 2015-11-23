package cn.dayutianfei.hdfs.rcfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HDFSClientDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://172.16.8.34:8020");
		// String path = "/temp/hadoop/conf/";
		// conf.addResource(new Path(path + "core-site.xml"));
		// conf.addResource(new Path(path + "hdfs-site.xml"));
		System.out.println(conf.get("fs.defaultFS"));
//		String filename = "/tmp/start_cm.sh";
		String filename = "/tmp/eclipse.tar.gz";
		System.out.println(HDFSClientDemo.GetAllNodeName(conf).toString());

		System.out.println(HDFSClientDemo.GetFileBolckHost(conf, filename)
				.toString());
	}

	// Get the locations of a file in the HDFS cluster
	public static List<String[]> GetFileBolckHost(Configuration conf,
			String FileName) {
		try {
			List<String[]> list = new ArrayList<String[]>();
			FileSystem hdfs = FileSystem.get(conf);
			Path path = new Path(FileName);
			FileStatus fileStatus = hdfs.getFileStatus(path);
			System.out.println("fileDesc:  blocksize="
					+ fileStatus.getBlockSize() + ", filesize="
					+ fileStatus.getLen() + "");
			BlockLocation[] blkLocations = hdfs.getFileBlockLocations(
					fileStatus, 0, fileStatus.getLen());
			int blkCount = blkLocations.length;
			System.out.println("blocknumber=" + blkCount + "");
			for (int i = 0; i < blkCount; i++) {
				String[] hosts = blkLocations[i].getHosts();

				// String[] blockNames = blkLocations[i].getNames();
				System.out.println("block " + i + " offset="
						+ blkLocations[i].getOffset() + " size=" +blkLocations[i].getLength());
				if (null != hosts && hosts.length > 0) {
					System.out.println("block " + i + "' host is:");
					for (String host : hosts) {
						System.out.print(host + " ");
					}
					System.out.println();
				} else {
					System.out.println("block " + i + "' cached on hosts!");
				}
				list.add(hosts);
			}
			return list;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String[] GetAllNodeName(Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			DistributedFileSystem hdfs = (DistributedFileSystem) fs;
			DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
			String[] names = new String[dataNodeStats.length];
			for (int i = 0; i < dataNodeStats.length; i++) {
				names[i] = dataNodeStats[i].getHostName();
				System.out.println(names[i]);
			}
			return names;
		} catch (IOException e) {
			System.out.println("error!!!!");
			e.printStackTrace();
		}
		return null;
	}
}
