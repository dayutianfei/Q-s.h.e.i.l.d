package cn.dayutianfei.loadserver.lib.serde;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 加载反序列化工具类
 * 对数据进行反序列化，用于客户端和数据节点之间的传输。
 * @author liubinbin
 * @version 0.2.6
 */
public class DataDeserialization {
    private static Logger LOG=LoggerFactory.getLogger(DataDeserialization.class);
	private static Schema _schema_file_set;
	private final String doc_set_name = "doc_set";

	@SuppressWarnings("unchecked")
	public List<List<Object>> deser(String db, String table, byte[] contents) throws Exception {
		LOG.debug("deser ...");
		List<List<Object>> _lines;
		long start=System.currentTimeMillis();
		try {
			if (null == _schema_file_set ) {
				_schema_file_set = SchemaManager.getSchema(SchemaManager.FILE_SET_SCHEMA);
			}
			Schema tableschema = SchemaManager.getSchema(db+table);
			_lines = new ArrayList<List<Object>>();

			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema_file_set);
			Decoder docs_decoder = DecoderFactory.get().binaryDecoder(contents, null);
			GenericRecord result = reader.read(null, docs_decoder);

			GenericData.Array<ByteBuffer> doc_set_s = (GenericData.Array<ByteBuffer>) result.get(doc_set_name);

			for (ByteBuffer buffer : doc_set_s) {
				DatumReader<GenericRecord> rc_reader = new GenericDatumReader<GenericRecord>(tableschema);
				Decoder rc_decoder = DecoderFactory.get().binaryDecoder(buffer.array(), null);
				GenericRecord rc_result = null;

				try {
					while (true) {
						List<Object> line = new ArrayList<Object>();

						rc_result = rc_reader.read(null, rc_decoder);
						for (Field field : tableschema.getFields()) {
							line.add(rc_result.get(field.name()));
						}
						_lines.add(line);
					}
				} catch (EOFException e) {

				}
			}

			doc_set_s.clear();
			result = null;
			docs_decoder = null;
			reader = null;
			LOG.debug("desc table:"+table+" cost time:"+(System.currentTimeMillis()-start));
		} catch (Exception e) {
			throw e;
		}

		LOG.debug("deser done.");
		return _lines;
	}
}
