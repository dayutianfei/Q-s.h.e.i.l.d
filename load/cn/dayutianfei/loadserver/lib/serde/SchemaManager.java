package cn.dayutianfei.loadserver.lib.serde;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.client.cache.ClientCache;

/**
 * Avro的Schema工具类，用于进行序列化和反序列化
 * 
 * @author liubinbin
 * @version 0.2.6
 *
 */
public class SchemaManager {
    private static Logger LOG=LoggerFactory.getLogger(SchemaManager.class);
    
	private static  HashMap<String, Schema> schema_cache = new HashMap<String, Schema>();
	private static Schema.Parser parser = new Schema.Parser();
	public static final String FILE_SET_SCHEMA = "file_set_schema";

	static {
		LOG.info("initing schema_cache");
		InputStream input_schema = null;
		Schema _schema = null;

		try {
			input_schema = SchemaManager.class.getResourceAsStream("avro.file.schema");
			_schema = parser.parse(input_schema);
		} catch (IOException e) {
			LOG.error("schema_cache init failed", e);
		}
		schema_cache.put(FILE_SET_SCHEMA, _schema);
		LOG.info("init schema_cache successed");
	}

	private static  Schema schema = null;

	/**
	 * 获取表下的Schema
	 * @param proto_name 使用dbName+tableName拼装的方式组成
	 * TODO: 考虑使用特殊分隔符间隔上述两个变量
	 * @return
	 */
	public synchronized static Schema getSchema(String proto_name) {
		if(null == schema_cache.get(proto_name)){
            Schema _schema = null;
			try {
			    if(ClientCache.tableCache.containsKey(proto_name)){
			        String jsonSchema = ClientCache.tableCache.get(proto_name).toSchema();
			        _schema = parser.parse(jsonSchema);
	                schema_cache.put(proto_name, _schema);
	                return _schema;
			    }
			} catch (Exception e) {
				LOG.error("getSchema " + proto_name + " , ERROR: " + e.getMessage());
			}
		}
		return schema_cache.get(proto_name);
	}
	
	public static Schema getSchema() {
		return schema;
	}

	public static void setSchema(Schema schema) {
		SchemaManager.schema = schema;
	}
}
