package cn.dayutianfei.jdbc.datatype;

import java.sql.Timestamp;

public class TimestampDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		String tsStr = "2011-05-09 11:49:45";
//		String tsStr = "11:49:45"	;
        try { 
        	//String的类型必须形如： yyyy-mm-dd hh:mm:ss[.f...] 这样的格式
        	//中括号部分的内容表示可选
            ts = Timestamp.valueOf(tsStr);  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
		System.out.println(ts.toString());
	}

}
