package cn.dayutianfei.hdfs.parquet;

import java.math.BigDecimal;

import parquet.example.data.simple.NanoTime;

//import org.apache.parquet.example.data.simple.NanoTime;

public class DateUtil {
	// / 从日期获得儒略日
	// / </summary>
	// / <param name="dt">yyyy-mm-dd</param>
	// / <returns>儒略日jd</returns>
	// TODO 格式校验
	public static int getMJd(String date) {
		String[] dt = date.split("-");
		long I = Long.parseLong(dt[0]);
		long J = Long.parseLong(dt[1]);
		long K = Long.parseLong(dt[2]);
		// 求出给定年(I)，月(J)，日(K)的儒略日:
		long jd = K - 32075 + 1461 * (I + 4800 + (J - 14) / 12) / 4 + 367
				* (J - 2 - (J - 14) / 12 * 12) / 12 - 3
				* ((I + 4900 + (J - 14) / 12) / 100) / 4;
		return (int) jd;
	}

	// 从儒略日转换为年月日(如把54863转换为2009-02-01)
	// <returns>年-月-日</returns>
	public static String getDateTimeByMJD(int mjd) {

		double JD = mjd;
		double Z = Math.floor(JD + 0.5);
		double W = Math.floor((Z - 1867216.25) / 36524.25);
		double X = Math.floor(W / 4);
		double AA = Math.floor(Z + 1 + W - X);
		double BB = Math.floor(AA + 1524);
		double CC = Math.floor((BB - 122.1) / 365.25);
		double DD = Math.floor(365.25 * CC);
		double EE = Math.floor((BB - DD) / 30.6001);
		double FF = Math.floor(30.6001 * EE);

		double Day = BB - DD - FF;
		double Month;
		double Year;

		if ((EE - 13) <= 12 && (EE - 13) > 0)
			Month = EE - 13;
		else
			Month = EE - 1;

		if (Month == 1 || Month == 2)
			Year = CC - 4715;
		else
			Year = CC - 4716;

		return String.format("%04d-%02d-%02d", (int) Year, (int) Month,
				(int) Day);
	}

	// hh:mm:ss.xxxxxxx
	// TODO 格式校验
	public static long getTimeOfDayNanos(String time) {
		String[] t = time.split("\\.");
		long nanos = 0;
		String[] t1 = t[0].split(":");
		nanos = ((Long.parseLong(t1[0]) * 60 + Long.parseLong(t1[1])) * 60 + Long
				.parseLong(t1[2])) * 1000000000;
		if (t.length == 1) {
			return nanos;
		} else {
			nanos = nanos
					+ (long) (Double.parseDouble("." + t[1]) * 1000000000);
			return nanos;
		}
	}

	public static String getTimeFromNanos(long nanos) {
		String time = null;
		long seconds = nanos / 1000000000;
		long nano = nanos % 1000000000;
		nano = (nano == 0.0d ? 0 : nano);
		long hour = seconds / 3600;
		long minute = (seconds - (3600 * hour)) / 60;
		long second = seconds - (hour * 3600 + minute * 60);
		time = String.format("%02d:%02d:%02d%s", hour, minute, second,
				(nano == 0.0 ? "" : "." + getNonas("" + nano, "1000000000")));
		return time;
	}

	public static String getDateTimeFromNanoTime(NanoTime nanoTime) {
		String dateTime = null;
		String date = getDateTimeByMJD(nanoTime.getJulianDay());
		String time = getTimeFromNanos(nanoTime.getTimeOfDayNanos());
		date = (date == null ? "" : (date + " "));
		time = (time == null ? "" : time);
		dateTime = date + time;
		return dateTime.trim();
	}

	public static void main(String[] args) {
		System.out.println(DateUtil.getDateTimeByMJD(2451545));
		System.out.println(DateUtil.getMJd("2000-01-01"));
		System.out.println(DateUtil.getTimeOfDayNanos("12:12:12.0000001"));
		System.out.println(DateUtil.getTimeFromNanos(43932000000100L));
		System.out.println(DateUtil.getDateTimeFromNanoTime(new NanoTime(
				2451545, 43932000000100L)));
		double d = 0.0000001;
		BigDecimal bigDecimal = new BigDecimal(d);
		System.out.println(d);
		System.out.println(bigDecimal.toString());

		System.out.println(getNonas("100", "1000000000"));
	}

	private static String getNonas(String nona, String x) {
		String[] aaa = new String[] { "", "0", "00", "000", "0000", "00000",
				"000000", "0000000", "00000000" };
		return aaa[x.length() - nona.length() - 1] + "1";
	}

}
