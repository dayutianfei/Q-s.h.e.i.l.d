package cn.dayutianfei.lusearch.server.searchmanager;

import org.apache.log4j.Logger;

public class LcSearchManagerCacheMgtThread implements Runnable {
	protected final static Logger LOG = Logger.getLogger(LcSearchManagerCacheMgtThread.class);
	
	public static boolean isUpdate =true;
	private static final int maxLivingTimeInSeconds = 60 * 5 ; // 缓存的句柄的最长存活时间
	
	public LcSearchManagerCacheMgtThread() {
	}
	
	@Override
	public void run() {
		while(isUpdate){
			try {
				LcSearchManager.update();
			} catch (Throwable e) {
				LOG.info("some is wrong when updating searchManager " + e.toString());
			}
			try {
				Thread.sleep(maxLivingTimeInSeconds * 1000);
			} catch (InterruptedException e) {
				LOG.info(e.toString());
			}
		}
	}

	public static void close() {
		isUpdate = false;
	}
}
