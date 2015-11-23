package cn.dayutianfei.loadserver.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.prototype.Field;
import cn.dayutianfei.loadserver.prototype.FieldTypeEnum;
import cn.dayutianfei.loadserver.prototype.Partition;
import cn.dayutianfei.loadserver.prototype.PartitionTypeEnum;
import cn.dayutianfei.loadserver.prototype.TableInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DataUtil {
    
	private static Logger LOG=LoggerFactory.getLogger(DataUtil.class);
	/**
	* @Title: objectList2StringArray 
	* @Description: TODO 根据表结构将List<Object> 转为String[]
	* 				因为数据与表信息里的字段对应关系可能为：
	* 			| field1  | field2(unable)| field3  | field4  | field5(unable)| field6  |				
	*  		    | list[0] |			      | list[1] | list[2] |               | list[3] |
	*  			所以需要将unable的字段对应位置填为null 
	* @param @param list
	* @param @param tableInfo
	* @param @return 
	* @return String[]
	* @throws
	 */
	public static String[] objectList2StringArray(List<Object> list, TableInfo tableInfo) {
		List<Field> fields=tableInfo.getFields();
		String[] record=new String[tableInfo.get_fieldTotalCount()];
		int index=0;
		for (int i = 0; i < record.length; i++) {
			if(fields.get(index).getFieldIndex()>i){
				record[i]="";
			}else{
				if(list.get(index) == null){
					record[i]="\\N";
				}else{
					if(fields.get(index).getType()==FieldTypeEnum.TYPE_BINARY){
						ByteBuffer bb=(ByteBuffer)list.get(index);
						byte[] binary=new byte[bb.capacity()];
						bb.get(binary, 0, binary.length);
						record[i]=new String(binary,Charset.forName("iso-8859-1"));
					}else{
						record[i]=""+list.get(index);
					}
				}
				index++;
			}
		}
		return record;
	}
	
	
	public static Map<String,List<String>> getNewRangMap(List<Map<String,List<String>>> rangeMaps, ArrayList<Partition> partitions){
		Map<String,List<String>> newRangeMap=Maps.newHashMap();	
		if(rangeMaps.size()>0){
			for (int i = 0; i < partitions.size(); i++) {
				if(partitions.get(i).getPartitionType() == PartitionTypeEnum.INTERVAL){
					List<String> list=Lists.newArrayList();
					String index=""+partitions.get(i).getPartitionField().getFieldIndex();
					for (int j = 0; j < rangeMaps.size(); j++){
						try {
							if(rangeMaps.get(j) == null || rangeMaps.get(j).get(index) == null){
								return null;
							}
							if(j == 0){
								list.addAll(rangeMaps.get(j).get(index));
								if(list.get(0).equals("")){
									break;
								}
							}else{
								List<String> oldList=rangeMaps.get(j).get(index);
								switch (partitions.get(i).getPartitionField().getType()) {
								case TYPE_TIMESTAMP:
									if(Long.parseLong(oldList.get(0))<Long.parseLong(list.get(0))){
										list.set(0, oldList.get(0));
									}
									if(Long.parseLong(oldList.get(1))>Long.parseLong(list.get(1))){
										list.set(1, oldList.get(1));
									}
									break;
								case TYPE_INT:
								case TYPE_TINYINT:
								case TYPE_INTEGER:
								case TYPE_SMALLINT:
								case TYPE_BIGINT:
									if(Long.parseLong(oldList.get(0))<Long.parseLong(list.get(0))){
										list.set(0, oldList.get(0));
									}
									if(Long.parseLong(oldList.get(1))>Long.parseLong(list.get(1))){
										list.set(1, oldList.get(1));
									}
									break;
								case TYPE_FLOAT:
								case TYPE_DOUBLE:
								case TYPE_DECIMAL:
								case TYPE_REAL:
									if(Double.parseDouble(oldList.get(0))<Double.parseDouble(list.get(0))){
										list.set(0, oldList.get(0));
									}
									if(Double.parseDouble(oldList.get(1))>Double.parseDouble(list.get(1))){
										list.set(1, oldList.get(1));
									}
									break;
								default:
									break;
								}
							}
						} catch (Exception e) {
							LOG.error("hzb>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>getNewRangMap error with rangeMaps:"+rangeMaps);
							LOG.error("hzb>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>getNewRangMap error with rangeMaps.get("+j+"):"+rangeMaps.get(j),e);
						}
					}
					newRangeMap.put(index, list);
				}

			}
			return newRangeMap;
		}else{
			return null;
		}
	}
	

}
