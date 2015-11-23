/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.dayutianfei.jdbc.datatype;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateFormat {

	public final static SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
	public final static SimpleDateFormat FORMAT2 = new SimpleDateFormat("yyyyMMdd");
	public final static SimpleDateFormat FORMAT4 = new SimpleDateFormat("yyyy/MM/dd");
	public final static SimpleDateFormat FORMAT6 = new SimpleDateFormat("yyyy/MM/dd HH");
	public final static SimpleDateFormat FORMAT8 = new SimpleDateFormat("yyyyMMddHH");
	public final static SimpleDateFormat FORMAT10 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	public final static SimpleDateFormat FORMAT12 = new SimpleDateFormat("yyyyMMddHHmmss");

	public static String longToDateString(final long timeStamp) {
		return FORMAT.format(new Date(timeStamp));
	}

	public static String longToDateString10(final long timeStamp) {
		return FORMAT10.format(new Date(timeStamp));
	}

	public static String longToDateString12(final long timeStamp) {
		return FORMAT12.format(new Date(timeStamp));
	}

	public static String longToDay(final long timeStamp) {
		return FORMAT2.format(new Date(timeStamp));
	}

	public static String longToDay4(final long timeStamp) {
		return FORMAT4.format(new Date(timeStamp));
	}

	public static String longToDay6(final long timeStamp) {
		return FORMAT6.format(new Date(timeStamp));
	}

	public static String longToHour(final long timeStamp) {
		return FORMAT8.format(new Date(timeStamp));
	}
	
	public static boolean isFORMAT12(String date){
		try {
			FORMAT12.parse(date);
		} catch (Exception e) {
			return false;
		}

		return true;		
	}
	
	public static boolean isFORMAT2(String date){
		try {
			FORMAT2.parse(date);
		} catch (Exception e) {
			return false;
		}

		return true;		
	}
	
	public static long getTimestamp12(String dateString) {
		long l = 0l;
		try {
			l = FORMAT12.parse(dateString).getTime();
		} catch (ParseException e) {
		}
		return l;
	}
	public static int getTimestamp10(String dateString) {
		int l = 0;
		try {
			l = (int)FORMAT12.parse(dateString).getTime()/1000;
		} catch (ParseException e) {
		}
		return l;
	}
	
	public static long getTodayLastTimestamp() {
		long l = 0l;
		try {
			l = FORMAT.parse(longToDay4(System.currentTimeMillis()) + " 23:59:59:999").getTime();
		} catch (ParseException e) {
		}
		return l;
	}

	public static long getHourLastTimestamp() {
		long l = 0l;
		try {
			l = FORMAT.parse(longToDay6(System.currentTimeMillis()) + ":59:59:999").getTime();
		} catch (ParseException e) {
		}
		return l;
	}

	public static long getOneHoursAgoTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.HOUR, -1);
//		String oneHoursAgoTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());// 获取到完整的时间
		return calendar.getTime().getTime();
	}
	
	public static void getTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.HOUR, -1);
		String date = calendar.getTime().getTime()+"";
		System.out.println(date);
		}
}
