package cn.dayutianfei.hive.meta;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class HiveMetaClientDemo {

	public static HiveMetaStoreClient client;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		HiveConf hiveConf = new HiveConf();
//		hiveConf.set("hive.metastore.uris", "thrift://172.16.8.110:9083,thrift://172.16.8.34:9083");
		hiveConf.set("hive.metastore.uris", "thrift://172.16.8.34:9083");
		hiveConf.setInt("hive.metastore.client.socket.timeout", 60);
		try {
			 client = new HiveMetaStoreClient(hiveConf);
			 System.out.println(client.getAllDatabases());
			 System.out.println(client.getAllTables("default"));
		} catch (MetaException e) {
			e.printStackTrace();
		}
	}

}
