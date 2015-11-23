package cn.dayutianfei.hive.meta;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaClientUsingHCat {
	private static final Logger logger = LoggerFactory
			.getLogger(HiveMetaClientUsingHCat.class);

	private static HiveMetaStoreClient client = null;

	private static synchronized HiveMetaStoreClient connect() {
		Configuration config = new Configuration();

		try {
			HiveConf hiveConf = HCatUtil.getHiveConf(config);
			return client = HCatUtil.getHiveClient(hiveConf);
		} catch (MetaException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static synchronized Table getTable(String dbName ,String tblName) {

		if (client == null) {
			client = connect();
			if (client == null) {
				return null;
			}
		}
		try {
			client.getAllDatabases();
		} catch (MetaException e) {
			logger.error("error: ", e);
			client = connect();
			if (client == null) {
				logger.error("meta store connect failed.");
				return null;
			}
		}
		synchronized (client) {
			try {
				Table table = HCatUtil
						.getTable(client, dbName, tblName);

				logger.info("Get metadata of \"" + table.getCompleteName()
						+ "\" successfully.");

				return table;
			} catch (TException e) {
				logger.error("Get metadata of \"" 
						+ "\" unsuccessfully: ", e);
			}
		}

		return null;
	}


//	public static synchronized void addPartition(Table tableDesc,
//			PartDesc partDesc) {
//		try {
//			if (partDesc.equalPartName != null
//					&& partDesc.equalPartName.length() != 0)
//				client.appendPartition(table., tableDesc.tblName,
//						partDesc.equalPartName);
//		} catch (AlreadyExistsException e) {
//			return;
//		} catch (Exception e) {
//			logger.error("add partition cause error: ", e);
//		}
//	}

	public static synchronized void invaild(String dbName, String tblName) {
		if (dbName == null || tblName == null) {
			return;
		}
		invaild(new Table(dbName, tblName));
	}

	public static synchronized void invaild(Table tableDesc) {
		
	}

}
