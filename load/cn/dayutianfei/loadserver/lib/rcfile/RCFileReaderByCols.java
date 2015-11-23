
package cn.dayutianfei.loadserver.lib.rcfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RCFileReaderByCols {
	protected static final Logger LOG = LoggerFactory.getLogger(RCFileReaderByCols.class);
	private RCFile.Reader reader;
	FileSystem fs;
	private List<Integer> columnIndexs;
	BytesRefArrayWritable cols = new BytesRefArrayWritable();
	
	/**
	 * @Title:	RCFileReaderByCols 
	 * @Description:	初始化  RCFileReaderByCols
	 * @param:	@param table 表信息
	 * @param:	@param path rcFile路径
	 * @param:	@param conf
	 * @param:	@param columnIndex 指定需要读取的列下标
	 * @throws
	 */
	public RCFileReaderByCols(Path path,Configuration conf,List<Integer> columnIndexs) {
		this.columnIndexs=columnIndexs;
		try {
			fs = FileSystem.get(conf);
			reader = new RCFile.Reader(fs, path, conf);
		} catch (IOException e) {
			LOG.error("init RCFileReader failed!", e);
		}
	}
	/**
	 * @Title: read 
	 * @Description: 读取多列数据 
	 * @param: @return    
	 * @return: List<String[]>    
	 * @throws 
	 */
	public List<String[]> read(){
		List<String[]> records=new ArrayList<String[]>();
		if (null==reader) {
			return null;
		}
		try {
			if(!reader.nextBlock()){
				return null;
			}else{
				for (Integer columnIndex : columnIndexs) {
					reader.getColumn(columnIndex.intValue(), cols);
					String[] record=new String[cols.size()];
					BytesRefWritable brw = null;
					for (int i = 0; i < cols.size(); i++) {
						brw=cols.get(i);
						record[i]=Bytes.toString(brw.getData(), brw.getStart(),
								brw.getLength());
					}
					records.add(record);
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
