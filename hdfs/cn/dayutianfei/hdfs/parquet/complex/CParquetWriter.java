package cn.dayutianfei.hdfs.parquet.complex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

public class CParquetWriter extends ParquetWriter<String[]> {
	public static final int BLOCK_SIZE = 128 * 1024 * 1024;

	public CParquetWriter(Path path, MessageType schema, WriteSupport<String[]> support,
			Configuration conf) throws IOException {
		this(path, schema, support, DEFAULT_IS_DICTIONARY_ENABLED, conf);
	}

	public CParquetWriter(Path path, MessageType schema, WriteSupport<String[]> support,
			boolean enableDictionary, Configuration conf) throws IOException {
		this(path, schema, support, CompressionCodecName.UNCOMPRESSED,
				enableDictionary, conf);
	}

	@SuppressWarnings({ "deprecation" })
	public CParquetWriter(Path path, MessageType schema, WriteSupport<String[]> support,
			CompressionCodecName codecName, boolean enableDictionary,
			Configuration conf) throws IOException {
		super(path, support,
				codecName, BLOCK_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE,
				enableDictionary, DEFAULT_IS_VALIDATING_ENABLED,
				DEFAULT_WRITER_VERSION, conf);
	}
}
