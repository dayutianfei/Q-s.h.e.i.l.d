package cn.dayutianfei.loadserver.prototype;

import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class Field implements Comparable<Field>{
	private String fieldName;//字段名
	private FieldTypeEnum type;//字段类型
	private String analyzer=null;//分词器
	private int fieldIndex;//字段下标
	private boolean enable=true;
	private String parameter = "-1";//varchar char string 长度限制 或decimal参数
	private boolean isNull;
	
	public Field() {
	}
	

	public Field(String fieldName,String _type,int fieldIndex,boolean isEnable) {
		this.fieldName = fieldName;
		this.type = FieldTypeEnum.str2FieldTypeEnum(_type);
		this.parameter = getLengthByType(_type);
		this.fieldIndex = fieldIndex;
		this.enable = isEnable;
	}


	private static String getLengthByType(String _type) {
		String regex = "\\((\\d+,?\\d*)\\)";
		Pattern pattern = Pattern.compile(regex);
		if(_type.contains("char") || _type.contains("string") || _type.contains("decimal")){
			Matcher matcher = pattern.matcher(_type);
			if(matcher.find()){
				String r=matcher.group(1);
//				System.out.println(r);
				return r;
			}
		}
		return "-1";
	}


	public int getFieldIndex() {
		return fieldIndex;
	}


	public void setFieldIndex(int fieldIndex) {
		this.fieldIndex = fieldIndex;
	}


	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public FieldTypeEnum getType() {
		return type;
	}
	public void setType(FieldTypeEnum type) {
		this.type = type;
	}
	public String getAnalyzer() {
		return analyzer;
	}
	public void setAnalyzer(String analyzer) {
		this.analyzer = analyzer;
	}


	public boolean isNull() {
		return isNull;
	}


	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}


	public boolean isEnable() {
		return enable;
	}


	public void setEnable(boolean enable) {
		this.enable = enable;
	}

	public String getParameter() {
		return parameter;
	}


	public void setParameter(String parameter) {
		this.parameter = parameter;
	}


	public String toString(){
		return "Field[Name="+fieldName+",Type="+type+",Index="+fieldIndex+",parameter="+parameter+",Enable="+enable+",Analyzer="+analyzer+"]";
	}


	@Override
	public int compareTo(Field arg0) {
		if(fieldIndex == arg0.getFieldIndex()){
			return 0;
		}else if(fieldIndex > arg0.getFieldIndex()){
			return 1;
		}else{
			return -1;
		}
	}
	
//	public static void main(String[] args) {
//		String[] str={"char()","varchar(22)","string(25)","char(3)","decimal(10)"};
//		for (int i = 0; i < str.length; i++) {
//			getLengthByType(str[i]);
//		}
//	}
}
