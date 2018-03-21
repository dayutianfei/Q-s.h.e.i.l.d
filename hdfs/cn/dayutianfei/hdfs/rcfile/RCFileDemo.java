package cn.dayutianfei.hdfs.rcfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;


/**
 * 
 * @author hzb
 * 
 */
public class RCFileDemo {

	public static void main(String[] args) throws IOException {
		conf = new Configuration();
		//"/user/root/data/test2/year=2015/month=11/day=10/nnn"
		String path = "/temp/rcfile/"+System.currentTimeMillis();
//		String path = "/temp/000000_0";
//		String path = "/home/egret/win7/000000_0";
		Path src = new Path(path);
		createRcFile(src, conf);
		readRcFile(src, conf);
	}

	private static Configuration conf;
	private static final String TAB = "\t";

	//测试数据
	private static String strings[] = { "1,beijing北京,123",
			"2,shandong輩經,12"+'\002'+"xxx"+'\001'+"345",
			"3,henan하하,24453",
			"4,hebeiСлужит для воспроизведения передачи громкого смеха,243423",
			"5,yunan,243" ,
			"6,tianjin,3333"};

	/**
	 * 生成一个RCF file
	 * 
	 * @param src
	 * @param conf
	 * @throws IOException
	 */
	private static void createRcFile(Path src, Configuration conf)
			throws IOException {
		conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, 3);// 列数
		conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, 4 * 1024 * 1024);// 决定行数参数一
		conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 3);// 决定行数参数二
		FileSystem fs = FileSystem.get(conf);
		RCFile.Writer writer = new RCFile.Writer(fs, conf, src);
		BytesRefArrayWritable cols = new BytesRefArrayWritable(3);// 列数，可以动态获取
		BytesRefWritable col = null;
		for (String s : strings) {
			String splits[] = s.split(",");
			int count = 0;
			for (String split : splits) {
				col = new BytesRefWritable(Bytes.toBytes(split), 0,
						Bytes.toBytes(split).length);
				cols.set(count, col);
				count++;
			}
			writer.append(cols);
		}
		writer.close();
		fs.close();
	}

	/**
	 * 读取解析一个RCF file
	 * 
	 * @param src
	 * @param conf
	 * @throws IOException
	 */
	private static void readRcFile(Path src, Configuration conf)
			throws IOException {
		// 需要获取的列，必须指定，具体看ColumnProjectionUtils中的设置方法
		ColumnProjectionUtils.setReadAllColumns(conf);
		FileSystem fs = FileSystem.get(conf);
		RCFile.Reader reader = new RCFile.Reader(fs, src, conf);
		readerByRow(reader);
//		readerByCol(reader);
		reader.close();
	}

	protected static void readerByRow(RCFile.Reader reader) throws IOException {
		// 已经读取的行数
		LongWritable rowID = new LongWritable();
		// 一个行组的数据
		BytesRefArrayWritable cols = new BytesRefArrayWritable();
		while (reader.next(rowID)) {
			reader.getCurrentRow(cols);
			// 包含一列的数据
			BytesRefWritable brw = null;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < cols.size(); i++) {
				brw = cols.get(i);
				// 根据start 和 length 获取指定行-列数据
//				System.out.println(brw.getBytesCopy());
				sb.append(Bytes.toString(brw.getData(), brw.getStart(),
						brw.getLength()));
				if (i < cols.size() - 1) {
					sb.append(TAB);
				}
			}
			System.out.println(sb.toString());
		}
	}

	protected static void readerByCol(RCFile.Reader reader) throws IOException {
		// 一个行组的数据
		BytesRefArrayWritable cols = new BytesRefArrayWritable();
		while (reader.nextBlock()) {
			for (int count = 0; count < 10; count++) {
				cols = reader.getColumn(count, cols);
				BytesRefWritable brw = null;
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < cols.size(); i++) {
					brw = cols.get(i);
					// 根据start 和 length 获取指定行-列数据
					sb.append(Bytes.toString(brw.getData(), brw.getStart(),
							brw.getLength()));
					if (i < cols.size() - 1) {
						sb.append(TAB);
					}
				}
				System.out.println(sb.toString());
			}
		}
	}

}


