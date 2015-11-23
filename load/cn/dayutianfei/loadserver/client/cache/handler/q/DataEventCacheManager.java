package cn.dayutianfei.loadserver.client.cache.handler.q;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.client.cache.DataEntity;

import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class DataEventCacheManager {
    
    private static Logger LOG=LoggerFactory.getLogger(DataEventCacheManager.class);
    
    private int BUFFER_SIZE = 1024 * 256; // RingBuffer 大小，必须是 2 的 N 次方；
    private RingBuffer<DataEntity> ringBuffer = null;
    private ExecutorService executorService = null;
    private Disruptor<DataEntity> disruptor = null;
    private final List<String> workers = new ArrayList<String>(Arrays.asList("only-worker"));
    
    @SuppressWarnings("unchecked")
    public void ini_one(){
        LOG.debug("DataCacheManager ini start...");
        executorService = Executors.newCachedThreadPool();
        disruptor = new Disruptor<DataEntity>(new DataEntityFactory(), BUFFER_SIZE, 
                executorService, ProducerType.SINGLE, new YieldingWaitStrategy());
        disruptor.handleExceptionsWith(new IgnoreExceptionHandler());
        disruptor.handleEventsWith(new DataEntityEventHandler(workers.get(0)));
        ringBuffer = disruptor.start();
    }
    
    public void ini_two(){
    }
    
    public void stop() {
        disruptor.shutdown();
        LOG.info("disruptor stopped");
        ExecutorsUtils.shutdownAndAwaitTermination(executorService, 60, TimeUnit.SECONDS);
        LOG.info("the executors stopped");
    }
    
    public RingBuffer<DataEntity> getRingBuffer() {
        return ringBuffer;
    }

    public void setRingBuffer(RingBuffer<DataEntity> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public static class ExecutorsUtils {
        public static void shutdownAndAwaitTermination(ExecutorService pool, int timeout, TimeUnit unit) {
            pool.shutdown(); // Disable new tasks from being submitted
            try {
                // Wait a while for existing tasks to terminate
                if (!pool.awaitTermination(timeout / 2, unit)) {
                    pool.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!pool.awaitTermination(timeout / 2, unit))
                        LOG.error("Pool did not terminate");
                }
            }
            catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                pool.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    public List<String> getWorkers() {
        return workers;
    }
}
