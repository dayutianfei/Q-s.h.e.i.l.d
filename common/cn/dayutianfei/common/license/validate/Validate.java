package cn.dayutianfei.common.license.validate;

import cn.dayutianfei.common.license.generator.SystemIdFactory;

import java.util.Date;

import org.apache.log4j.Logger;


public class Validate {
	protected final static Logger LOG = Logger.getLogger(Validate.class);
	
	public static boolean systemIdValidate(String systemId) {
		LOG.info("sysid:"+systemId != null && systemId.equals(new SystemIdFactory().generateSystemId()));
		return systemId != null && systemId.equals(new SystemIdFactory().generateSystemId());
	}
	
	public static boolean isOutOfDate(String startTimeStr, String timeLimitStr, String runnedTimeStr) {
		long startTime = Long.parseLong(startTimeStr);
		long timeLimit = Long.parseLong(timeLimitStr);
		long runnedTime = Long.parseLong(runnedTimeStr);
		long now = System.currentTimeMillis();
		LOG.info("startTime" + startTime);
		LOG.info("timeLimit" + timeLimit);
		LOG.info("runnedTime" + runnedTime);
		LOG.info("now" + now);
		return startTime + timeLimit > now && timeLimit > runnedTime;
	}
	
	public static void main(String[] args) {
		LOG.info(System.currentTimeMillis());
		Date date = new Date();
		date.setTime(System.currentTimeMillis());
		LOG.info(date);
	}
}
