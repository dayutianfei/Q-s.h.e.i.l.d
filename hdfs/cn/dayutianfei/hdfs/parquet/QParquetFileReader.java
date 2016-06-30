package cn.dayutianfei.hdfs.parquet;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// parquet
//import org.apache.parquet.column.ColumnDescriptor;
//import org.apache.parquet.example.data.Group;
//import org.apache.parquet.example.data.simple.NanoTime;
//import org.apache.parquet.hadoop.ParquetFileReader;
//import org.apache.parquet.hadoop.ParquetReader;
//import org.apache.parquet.hadoop.example.GroupReadSupport;
//import org.apache.parquet.hadoop.metadata.ParquetMetadata;
//import org.apache.parquet.io.api.Binary;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

// hadoop-parquet
import parquet.column.ColumnDescriptor;
import parquet.example.data.Group;
import parquet.example.data.simple.NanoTime;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.api.Binary;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;


public class QParquetFileReader {
	protected static final Logger LOG = LoggerFactory.getLogger(QParquetFileReader.class);
	ParquetReader<Group> reader = null;
	MessageType schema = null;
	String filePathStr = null;
	private List<ColumnDescriptor> cols;
	@SuppressWarnings("deprecation")
	public QParquetFileReader(Path parquetFilePath) throws IOException  {
		filePathStr = parquetFilePath.toString();
		Configuration configuration = new Configuration();
	    GroupReadSupport readSupport = new GroupReadSupport();
	    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
	    schema = readFooter.getFileMetaData().getSchema();
	    System.out.println(schema.toString());
	    System.out.println(readFooter.getFileMetaData().getKeyValueMetaData());
	    this.cols = schema.getColumns();
	    readSupport.init(configuration, null, schema);
//	    UnboundRecordFilter unboundRecordFilter = null;
		reader = new ParquetReader<Group>(parquetFilePath, readSupport);
	}
	public String[] read(){
		String[] records = new String[schema.getFieldCount()];
		if (reader == null) {
			return null;
		}
		try {
			Group g = null;
			if ((g = reader.read())!=null) {
				for (int i = 0; i < records.length; i++) {
					try {
						if (cols.get(i).getType()==PrimitiveTypeName.INT96) {
							Binary binary = g.getInt96(i, 0);
							if (binary==null) {
								records[i]=String.valueOf(null);
							}else {
								NanoTime nanoTime = NanoTime.fromBinary(binary);
								records[i] = DateUtil.getDateTimeFromNanoTime(nanoTime);
							}
						}else if (cols.get(i).getType()==PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
							Binary binary = g.getBinary(i, 0);
							int scale = schema.getFields().get(i).asPrimitiveType().getDecimalMetadata().getScale();
							HiveDecimal hiveDecimal = new HiveDecimalConvert(binary.getBytes(),scale).getHiveDecimal();
							records[i] = hiveDecimal.toString();
						}else{
							records[i] = g.getValueToString(i, 0);
						}
					} catch (Exception e) {
						records[i] = "\\N";
						continue;
					}
				}
				return records;
			}else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("read ParquetFile["+filePathStr+"] failed!", e);
		}
		return null;
	}
	
	public void close(){
		if (reader!=null) {
			try {
				reader.close();
			} catch (IOException e) {
				LOG.error("close parquet Reader for ["+filePathStr+"] faild!",e);
			}
		}
	}
	
}
