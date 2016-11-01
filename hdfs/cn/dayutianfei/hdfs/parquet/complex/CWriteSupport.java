package cn.dayutianfei.hdfs.parquet.complex;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;

public class CWriteSupport extends WriteSupport<String[]> {
	private final static Logger LOG = LoggerFactory
			.getLogger(CWriteSupport.class);
	private MessageType schema;
	private RecordConsumer recordConsumer;

	public CWriteSupport(MessageType schema) {
		this.schema = schema;
	}

	@Override
	public WriteSupport.WriteContext init(
			Configuration arg0) {
		return new WriteContext(schema, new HashMap<String, String>());
	}

	@Override
	public void prepareForWrite(RecordConsumer r) {
		this.recordConsumer = r;

	}

	@Override
	public void write(String[] record) {
		try {
			// 仅测试,只有四个字段,且不对数据的正确性进行校验
			// 必须保证startField()/endField()间有数据写入。
			// 当startGroup()/endGroup()间无写入时，实际插入null;
			// column a:int
			recordConsumer.startMessage();
			String value1 = record[0];
			if (value1 != null && !value1.equals("")) {
				recordConsumer.startField("a", 0);
				recordConsumer.addInteger(Integer.parseInt(value1));
				recordConsumer.endField("a", 0);
			}
			// column b:list<string>
			String value2 = record[1];
			if (value2 != null && !value2.equals("")) {
				// 规定以','为array元素的分隔符
				String array[] = value2.split(",");
				if (array.length > 0) {
					recordConsumer.startField("b", 1);
					recordConsumer.startGroup();
					recordConsumer.startField("bag", 0);
					for (String element : array) {
						if (element != null) {
							recordConsumer.startGroup();
							recordConsumer.startField("array_element", 0);
							recordConsumer.addBinary(stringToBinary(element));
							recordConsumer.endField("array_element", 0);
							recordConsumer.endGroup();
						}
					}
					recordConsumer.endField("bag", 0);
					recordConsumer.endGroup();
					recordConsumer.endField("b", 1);
				}
			}
			// column c:map<string, string>
			String value3 = record[2];
			if (value3 != null && !value3.equals("")) {
				// 规定以','为map元素的分隔符,':'为key value的分隔符
				String array[] = value3.split(",");
				recordConsumer.startField("c", 2);
				recordConsumer.startGroup();
				recordConsumer.startField("map", 0);
				for (String element : array) {
					String keyValue[] = element.split(":");
					if (keyValue.length == 2) {
						recordConsumer.startGroup();
						recordConsumer.startField("key", 0);
						recordConsumer.addBinary(stringToBinary(keyValue[0]));
						recordConsumer.endField("key", 0);
						recordConsumer.startField("value", 1);
						recordConsumer.addBinary(stringToBinary(keyValue[1]));
						recordConsumer.endField("value", 1);
						recordConsumer.endGroup();
					}
				}
				recordConsumer.endField("map", 0);
				recordConsumer.endGroup();
				recordConsumer.endField("c", 2);
			}
			// column d : struct<int,float>
			String value4 = record[3];
			if (value4 != null && !value4.equals("")) {
				// 规定以','为struct元素的分割符
				String array[] = value4.split(",");
				if (array.length == 2) {
					recordConsumer.startField("d", 3);
					recordConsumer.startGroup();
					recordConsumer.startField("number", 0);
					recordConsumer.addInteger(Integer.parseInt(array[0]));
					recordConsumer.endField("number", 0);
					recordConsumer.startField("score", 1);
					recordConsumer.addFloat(Float.parseFloat(array[1]));
					recordConsumer.endField("score", 1);
					recordConsumer.endGroup();
					recordConsumer.endField("d", 3);
				}
			}
			recordConsumer.endMessage();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private Binary stringToBinary(Object value) {
		return Binary.fromString(value.toString());
	}
}
