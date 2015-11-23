package cn.dayutianfei.loadserver.prototype;

import java.util.List;

public class Partition {
	private Field partitionField;//分区字段
	private PartitionTypeEnum partitionType;//分区类型
	private int partitionArgv;//分区参数
	
	public Partition(Field field,PartitionTypeEnum type,int argv){
		this.partitionField=field;
		this.partitionType=type;
		this.partitionArgv=argv;
	}
	
	/**
	 * TODO:暂未有分区
	 * @param line
	 * @return
	 */
	public static String calcPartition(List<Partition> part, List<Object> line){
	    return "none";
	}
	
	public Field getPartitionField() {
		return partitionField;
	}
	public void setPartitionField(Field partitionField) {
		this.partitionField = partitionField;
	}
	public PartitionTypeEnum getPartitionType() {
		return partitionType;
	}
	public void setPartitionType(PartitionTypeEnum partitionType) {
		this.partitionType = partitionType;
	}
	public int getPartitionArgv() {
		return partitionArgv;
	}
	public void setPartitionArgv(int partitionArgv) {
		this.partitionArgv = partitionArgv;
	}
	
	
	public String toString(){
		return "Partition[partitionField="+partitionField.getFieldName()+",Type="+partitionType+",Arg="+partitionArgv+"]";
	}
	
}