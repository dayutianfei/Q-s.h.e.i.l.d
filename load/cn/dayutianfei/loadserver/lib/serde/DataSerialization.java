package cn.dayutianfei.loadserver.lib.serde;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 加载序列化工具类
 * 对数据进行序列化，用于客户端和数据节点之间的传输。
 * @author wzy
 *
 */
public class DataSerialization{
    private static Logger LOG=LoggerFactory.getLogger(DataSerialization.class);
	
	private final Schema _schema_file_set = SchemaManager.getSchema(SchemaManager.FILE_SET_SCHEMA);
	private final String doc_set_name = "doc_set";
	
	public byte[] serialize(Schema tableschema, List<List<Object>> _lines) throws Exception {
		LOG.debug("begin to serialize data");
		List<ByteArrayOutputStream> rc_outs = new ArrayList<ByteArrayOutputStream>();
		 List<Field> fields=tableschema.getFields();
		 
		for (List<Object> line:_lines) {
			GenericRecord rc = new GenericData.Record(tableschema);
			for (int i=0;i<fields.size();i++) {
				if(fields.get(i).schema().getType().getName().equals("bytes")){
					try {
						rc.put(fields.get(i).name(),  ByteBuffer.wrap((byte[])line.get(i)));
					} catch (Exception e) {
						throw new Exception("There is a CAST ERROR !!");
					}
				}else{
					rc.put(fields.get(i).name(), line.get(i).toString());
				}
			}
			ByteArrayOutputStream rc_out = new ByteArrayOutputStream();
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(tableschema);
			Encoder encoder = EncoderFactory.get().binaryEncoder(rc_out, null);
			writer.write(rc, encoder);
			encoder.flush();
			rc_out.close();
			rc_outs.add(rc_out);
		}
		
		GenericRecord docs_datum = new GenericData.Record(_schema_file_set);
		Schema doc_set = _schema_file_set.getField(doc_set_name).schema();
		GenericData.Array<ByteBuffer> doc_set_datum = new GenericData.Array<ByteBuffer>(5, doc_set);
		for (ByteArrayOutputStream out : rc_outs) {
			doc_set_datum.add(ByteBuffer.wrap(out.toByteArray()));
		}
		rc_outs.clear();
		docs_datum.put(doc_set_name, doc_set_datum);

		ByteArrayOutputStream docs_out = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> docs_writer = new GenericDatumWriter<GenericRecord>(_schema_file_set);
		Encoder docs_encoder = EncoderFactory.get().binaryEncoder(docs_out, null);
		try {
			docs_writer.write(docs_datum, docs_encoder);
			docs_encoder.flush();
			docs_out.close();
		} catch (IOException e) {
		}
		doc_set_datum.clear();

		return docs_out.toByteArray();
	}
}
