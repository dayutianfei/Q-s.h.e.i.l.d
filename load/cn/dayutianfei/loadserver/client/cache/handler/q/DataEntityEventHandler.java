package cn.dayutianfei.loadserver.client.cache.handler.q;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.common.hash.HashUtil;
import cn.dayutianfei.loadserver.client.cache.ClientCache;
import cn.dayutianfei.loadserver.client.cache.DataEntity;
import cn.dayutianfei.loadserver.client.deliver.ConnectionCollection;
import cn.dayutianfei.loadserver.client.message.DataMessage;
import cn.dayutianfei.loadserver.common.Codes;
import cn.dayutianfei.loadserver.lib.serde.DataSerialization;
import cn.dayutianfei.loadserver.lib.serde.SchemaManager;
import cn.dayutianfei.loadserver.prototype.Partition;
import cn.dayutianfei.loadserver.prototype.TableInfo;

import com.lmax.disruptor.EventHandler;

/**
 * 通过实现接口 com.lmax.disruptor.EventHandler<T> 定义事件处理的具体实现
 * @author wzy
 */
public class DataEntityEventHandler implements EventHandler<DataEntity>{
    
    private static Logger LOG=LoggerFactory.getLogger(DataEntityEventHandler.class);
    
    private String workName = "" ;
    private AtomicLong totalAdded = new AtomicLong(0);   // 从环中读取的数据
    private AtomicLong totalSent = new AtomicLong(0);       // 累计发送的数据
    //String:唯一标识，使用“数据库名+表名+分区名“进行组合，使用冒号间隔
    private ConcurrentHashMap<String, ArrayList<List<Object>>> buffer = null;
    private AtomicLong count = new AtomicLong(1);
    private AtomicLong deliveryTimes = new AtomicLong(1);
    private AtomicLong mark = new AtomicLong(1);
    private final int maxBuffered = 1024 * 2;
    private Random random = new Random();
    private DataSerialization dataAssist = null;
    
    public DataEntityEventHandler(String workName){
        this.workName = workName;
        this.buffer = new ConcurrentHashMap<String, ArrayList<List<Object>>>();
        dataAssist = new DataSerialization();
        LOG.info("worker:"+workName+"started");
    }
    @Override
    public void onEvent(DataEntity event, long sequence, boolean endOfBatch) throws Exception {
        //System.out.println(workName + "Event: " + event.getDb() + " tableName:" + event.getTblName() + "data:");
        //计算数据所属分区，发到指定的Connection的容器中
        if("load" == event.getOpera()){
            String db = event.getDb();
            String tableName = event.getTblName();
            List<Object> data = event.getData();
            String partName = getPartitionName(db,tableName, data); //XXX
            String identifiedSign = db+":"+tableName+":"+partName;
            totalAdded.incrementAndGet();
            if(!buffer.contains(identifiedSign)){
                buffer.putIfAbsent(identifiedSign, new ArrayList<List<Object>>());
            }
            buffer.get(identifiedSign).add(data);
            LOG.debug("count:"+count);
            if(count.getAndIncrement()%maxBuffered==0){
                LOG.info("start to send "+mark.getAndIncrement());
                synchronized (buffer) {
                    LOG.info(workName +"one:" +" contains "+buffer.size()+" targets to send...");
                    this.deliver(buffer);
                    buffer.clear();
                }
            }
        }else{//处理flush/commit操作
            if("flush".equals(event.getOpera())){
                LOG.info(workName +"one:flush"+buffer.size());
                deliver(buffer);
                buffer.clear();
                LOG.info("total added---->>  " + totalAdded);
                LOG.info("total sent------->>  " + totalSent);
            }
        }
    }
    
    //与后端服务进行数据的传输，包括传输之前的序列化操作
    private void deliver(ConcurrentHashMap<String, ArrayList<List<Object>>> currentBuffer ){
        if(null == currentBuffer || currentBuffer.isEmpty()){
            return;
        }
        for(String key: currentBuffer.keySet()){
            String[] keys = key.split(":"); // 拆分数据库、表、分区信息
            ArrayList<List<Object>> dataSet = currentBuffer.get(key);
            if(null == dataSet || dataSet.isEmpty()){
                return;
            }
            HashMap<String, String> heads = new HashMap<String,String>();
            heads.put(DataMessage.HEAD_DB, keys[0]);
            heads.put(DataMessage.HEAD_TBL, keys[1]);
            heads.put(DataMessage.HEAD_OP, Codes.LOAD_DATA);
            heads.put(DataMessage.HEAD_CHANNEL, "cc"+(deliveryTimes.getAndIncrement()%2+1));
            byte[] dataContent = null;
            try {
                dataContent = dataAssist.serialize(SchemaManager.getSchema(keys[0]+keys[1]), dataSet);
//              Connection messager = ConnectionCollection.getHTTPCollection(getHost(keys[0],keys[1],keys[2]), ClientCache.HTTP_SOURCE_PORT);
                try {
                    ConnectionCollection.getAvroCollection(getHost(keys[0],keys[1],keys[2]), ClientCache.HTTP_SOURCE_PORT).flush(heads, dataContent);
//                    ConnectionCollection.getThriftCollection(getHost(keys[0],keys[1],keys[2]), ClientCache.HTTP_SOURCE_PORT).flush(heads, dataContent);
                    totalSent.getAndAdd(dataSet.size());
                }catch (IOException e) {
                    e.printStackTrace();
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    private String getPartitionName(String dbName, String tableName, List<Object> line){
        TableInfo table = ClientCache.getTableInfo(dbName, tableName);
        ArrayList<Partition> partitions = table.getPartitions();
        String partition = null;
        try {
            partition = Partition.calcPartition(partitions, line);
        }
        catch (Exception e) {
            return "none";
        }
        return partition;
    }
    
    private String getHost(String db, String tbl, String part){
        int nodeIndex = 0;
        if(null != part && part.trim().length() !=0 && !"none".equals(part)){
            try {
                nodeIndex = Math.abs(HashUtil.crc32Hash(part.getBytes("utf8"))) % ClientCache.hostName.size();
            }
            catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }else{
            nodeIndex = this.random.nextInt(ClientCache.hostName.size());
        }
        return ClientCache.hostName.get(nodeIndex);
    }
}