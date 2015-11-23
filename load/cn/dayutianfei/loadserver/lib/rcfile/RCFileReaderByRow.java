package cn.dayutianfei.loadserver.lib.rcfile;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName:	RCFileReader 
 * @Description:RCFile读取类 
 * @author:	hzb
 * @date:	2015年6月25日 下午3:41:02 
 *
 */
public class RCFileReaderByRow {
	protected static final Logger LOG = LoggerFactory.getLogger(RCFileReaderByRow.class);
	private RCFile.Reader reader;
	private FileSystem fs;
	private LongWritable rowID=new LongWritable();
	BytesRefArrayWritable cols = new BytesRefArrayWritable();

	/**
	 * @Title:	RCFileReaderByRow 
	 * @Description:	初始化RCFileReaderByRow 
	 * @param:	@param table 表信息
	 * @param:	@param path rcFile路径
	 * @param:	@param conf
	 * @param:	@param ids 指定需要读取的列
	 * @throws
	 */
	public RCFileReaderByRow(Path path,Configuration conf,List<Integer> ids) {
		if(ids!=null){
			ColumnProjectionUtils.appendReadColumns(conf, ids);
		}else{
			ColumnProjectionUtils.setReadAllColumns(conf);
		}
		try {
			fs = FileSystem.get(conf);
			reader = new RCFile.Reader(fs, path, conf);
		} catch (IOException e) {
			LOG.error("init RCFileReader failed!", e);
		}
	}

	/**
	 * @Title: read 
	 * @Description: 读取一行数据 
	 * @param: @return    
	 * @return: String[]    
	 * @throws 
	 
	 */
	public String[] read(){
		String[] records;
		if (null==reader) {
			return null;
		}
		try {
			if(!reader.next(rowID)){
				return null;
			}else{
				reader.getCurrentRow(cols);
				// 包含一列的数据
				BytesRefWritable brw = null;
				records=new String[cols.size()];
				for (int i = 0; i < cols.size(); i++) {
					brw = cols.get(i);
					// 根据start 和 length 获取指定行-列数据
					records[i]=Bytes.toString(brw.getData(), brw.getStart(),brw.getLength());
				}
				return records;
			}
		} catch (IOException e) {
			LOG.error("read RCFile failed!", e);
		}
		return null;
		
	}
	
	
	/**
	 * @Title: close 
	 * @Description: 关闭文件 
	 * @param:     
	 * @return: void    
	 * @throws 
	 
	 */
	public void close(){
		if(null!=reader){
			reader.close();
		}
	}
	
}
