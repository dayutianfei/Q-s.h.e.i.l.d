package cn.dayutianfei.jdbc.datatype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class String2TimeTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// data preparation
		String dateInput = "20150401";
		List<String> timeStringLists = new ArrayList<String>();
		int count = 0;
		for(int hour = 0;hour < 24; hour++){
			for(int min = 0;min < 60; min++){
				for(int sec = 0;sec < 60; sec++){
					String _hour =""+ hour;
					if(hour<10){
						_hour ="0"+hour;
					}
					String _min =""+ min;
					if(min<10){
						_min="0"+min;
					}
					String _sec =""+ sec;
					if(sec<10){
						_sec ="0"+sec;
					}
					count++;
					timeStringLists.add(dateInput+_hour+_min+_sec);
				}
			}
		}
		System.out.println("done with build the data " + count);
		Map<String, ArrayList<String>> partition2Data = new HashMap<String,ArrayList<String>>();
		Map<String, ArrayList<String>> partition2Data2 = new HashMap<String,ArrayList<String>>();
		// change time to timestamp and set 3600 per partition
		long t1 = System.currentTimeMillis();
		long chk1 = 0l;
		for(String currentTime : timeStringLists){
			long tt = System.currentTimeMillis();
			int _curr = DateFormat.getTimestamp10(currentTime);
			chk1 += System.currentTimeMillis() - tt;
			String part = ""+_curr%3600;
			System.out.println(part);
			if(partition2Data.containsKey(part)){
				partition2Data.get(part).add(currentTime);
			}else{
				ArrayList<String> toadd = new ArrayList<String>();
				toadd.add(currentTime);
				partition2Data.put(part, toadd);
			}
		}
		System.out.println(partition2Data.keySet().size());
		System.out.println(chk1);
		System.out.println(System.currentTimeMillis()- t1);
		long t2= System.currentTimeMillis();
		
		// sub
		long chk2 = 0l;
		for(String currentTime : timeStringLists){
			//int _curr = DateFormat.getTimestamp10(currentTime);
			long tt = System.currentTimeMillis();
			String part = ""+currentTime.substring(10);
			chk2 += System.currentTimeMillis() - tt;
			if(partition2Data2.containsKey(part)){
				partition2Data2.get(part).add(currentTime);
			}else{
				ArrayList<String> toadd = new ArrayList<String>();
				toadd.add(currentTime);
				partition2Data2.put(part, toadd);
			}
		}
		System.out.println(partition2Data2.keySet().size());
		System.out.println(chk2);
		System.out.println(System.currentTimeMillis()- t2);
	}
}
