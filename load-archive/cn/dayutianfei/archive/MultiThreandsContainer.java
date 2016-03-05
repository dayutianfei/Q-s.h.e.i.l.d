package cn.dayutianfei.archive;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.utils.ThreadsUtil;

/**
 * 该类实现多线程处理框架
 * @author dayutianfei
 *
 */
public class MultiThreandsContainer {
	protected static Logger LOG=LoggerFactory.getLogger(MultiThreandsContainer.class);
	private ExecutorService executorService = null;	// 线程池
	private static int THREAD_NUMBER_IN_POOL = 10;	// 最大线程数
	public static AtomicLong jobHasDone = null;		// 完成的消息数
	public static ConcurrentLinkedQueue<String> msg = null;			// 消息队列
	public static ConcurrentHashMap<String, AtomicLong> jobStatis = null; 	// 每个线程消息处理统计
	
	
	public void init(int threadNumber){
		// 模拟加载数据
		msg = new ConcurrentLinkedQueue<String>();
		for(int st=0;st<100;st++){
			msg.add("msg"+st);
		}
		
		jobStatis = new ConcurrentHashMap<String,AtomicLong>();
		jobHasDone = new AtomicLong(0);
		if(threadNumber>0){
			THREAD_NUMBER_IN_POOL = threadNumber;
		}
		executorService = Executors.newFixedThreadPool(THREAD_NUMBER_IN_POOL);
	}
	
	public void startToWork(){
		for(int i=0;i<THREAD_NUMBER_IN_POOL;i++){
			executorService.submit(new ThreadTaskHandler());
		}
	}
	
	public void close(){
		LOG.info("container begin to stop...");
		ThreadsUtil.shutdownAndAwaitTermination(executorService, 60, TimeUnit.SECONDS);
        LOG.info("the executors in the container has stopped");
	}
	
	
}
