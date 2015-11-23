package cn.dayutianfei.loadserver.client;

import java.util.List;

import org.apache.hive.com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.RingBuffer;

import cn.dayutianfei.loadserver.client.cache.ClientCache;
import cn.dayutianfei.loadserver.client.cache.DataEntity;
import cn.dayutianfei.loadserver.client.cache.handler.DataEventCacheManager;
import cn.dayutianfei.loadserver.client.deliver.ConnectionCollection;

/**
 * 能自动重连、负载均衡的加载客户端
 * @author wzy
 *
 */
public class BalancedClient extends Client {
    
    private DataEventCacheManager dm = null;
    
    /**
     * 初始化构造方法
     * @param hostNameOrIp  加载初始化时接入的主机名或IP
     * @param port                        端口号
     * @param timeoutInSecond   后端交互的超时时间，单位为秒
     */
    public BalancedClient(String hostNameOrIp, int port, int timeoutInSecond){
        super(hostNameOrIp,port,timeoutInSecond);
        dm = new DataEventCacheManager();
        dm.ini_one();
        ClientCache.updateCache();
        ConnectionCollection.ini();
    }
    
    @Override
    public boolean insert(String dbName, String tblName, List<Object> data) throws Exception {
        boolean re = false;
        //判定用户写入的记录是否合法
        if(null==data || data.isEmpty() || !ClientCache.tableCache.containsKey(dbName+tblName)){
            return re;
        }
        //将数据写入RB中
        RingBuffer<DataEntity> ringBuffer = dm.getRingBuffer();
        long pos= ringBuffer.next();
        try{
            ringBuffer.get(pos).setDb(dbName);
            ringBuffer.get(pos).setTblName(tblName);
            ringBuffer.get(pos).setOpera("load");
            ringBuffer.get(pos).setData(data);
            ringBuffer.get(pos).setPartName("");
            ringBuffer.get(pos).setOperators(dm.getWorkers());
            re = true;
        }catch(Exception e){
            re =  false;
            throw new Exception("error occur when loading data...");
        }finally{
            ringBuffer.publish(pos);
        }
        return re;
    }

    @Override
    public void commit() throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void flush() throws Exception {
        for(String worker: dm.getWorkers()){
          //将数据写入RB中
            Log.info(worker+" need to flush");
            RingBuffer<DataEntity> ringBuffer = dm.getRingBuffer();
            long pos= ringBuffer.next();
            try{
                ringBuffer.get(pos).setOpera("flush");
                ringBuffer.get(pos).setOperators(dm.getWorkers());
            }catch(Exception e){
                throw new Exception("error occur when loading data...");
            }finally{
                ringBuffer.publish(pos);
            }
        }
    }

    @Override
    public void close() throws Exception {
        dm.stop();
    }

    @Override
    public void reloadConf(String host, int port, int timeoutInSeconds) throws Exception {
        // TODO Auto-generated method stub
    }
}
