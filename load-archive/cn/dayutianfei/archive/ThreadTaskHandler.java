package cn.dayutianfei.archive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadTaskHandler implements Runnable{

	protected static Logger LOG=LoggerFactory.getLogger(ThreadTaskHandler.class);

	@Override
	public void run() {
		try{
			while(true){
				String msg = MultiThreandsContainer.msg.poll();
				if(null==msg){
					break;
				}
				LOG.info("msg content :" + msg);
				Thread.sleep(1);
			}
		}catch(InterruptedException e){
			//do nothing
		}
		MultiThreandsContainer.jobHasDone.incrementAndGet();
		LOG.info("SUMMARY: " + " tasks is done by "+ Thread.currentThread().getId());
	}

}
