package cn.dayutianfei.loadserver.client.cache.handler;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

import com.lmax.disruptor.WorkHandler;

/**
 * 通过实现接口 com.lmax.disruptor.EventHandler<T> 定义事件处理的具体实现
 * @author wzy
 */
public class DataEntityHandler implements WorkHandler<DataEntity>{
    
    private static Logger LOG=LoggerFactory.getLogger(DataEntityHandler.class);
    
    private String workName = "" ;
    private AtomicLong totalAdded = new AtomicLong(0);   // 从环中读取的数据
    private AtomicLong totalSent = new AtomicLong(0);       // 累计发送的数据
    //String:唯一标识，使用“数据库名+表名+分区名“进行组合，使用冒号间隔
    private ConcurrentHashMap<String, ArrayList<List<Object>>> buffer_one = null;
    private ConcurrentHashMap<String, ArrayList<List<Object>>> buffer_two = null;
    private AtomicLong count = new AtomicLong(1);
    private AtomicLong deliveryTimes = new AtomicLong(1);
    private AtomicLong mark = new AtomicLong(0);
    private static ConcurrentHashMap<String,AtomicInteger> coordinator = new ConcurrentHashMap<String,AtomicInteger>();
    private final int maxBuffered = 1024 * 4;
    private Random random = new Random();
    private DataSerialization dataAssist = null;
    
    public DataEntityHandler(String workName){
        this.workName = workName;
        this.buffer_one = new ConcurrentHashMap<String, ArrayList<List<Object>>>();
        this.buffer_two = new ConcurrentHashMap<String, ArrayList<List<Object>>>();
        coordinator.put(workName, new AtomicInteger(0));
        dataAssist = new DataSerialization();
        LOG.info("worker:"+workName+"started");
    }
    @Override
    public void onEvent(DataEntity event) throws Exception {
        //System.out.println(workName + "Event: " + event.getDb() + " tableName:" + event.getTblName() + "data:");
        //计算数据所属分区，发到指定的Connection的容器中
        if("load" == event.getOpera()){
            String db = event.getDb();
            String tableName = event.getTblName();
            List<Object> data = event.getData();
            String partName = getPartitionName(db,tableName, data); //XXX
            String identifiedSign = db+":"+tableName+":"+partName;
            totalAdded.incrementAndGet();
            if(mark.get()%2==0){
                if(!buffer_one.contains(identifiedSign)){
                    buffer_one.putIfAbsent(identifiedSign, new ArrayList<List<Object>>());
                }
                buffer_one.get(identifiedSign).add(data);
                LOG.debug("count:"+count);
                if(count.getAndIncrement()%maxBuffered==0){
                    LOG.info("mark changed to "+mark.incrementAndGet());
                    synchronized (buffer_one) {
                        LOG.info(workName +"one:" +" contains "+buffer_one.size()+" targets to send...");
                        this.deliver(buffer_one);
                        buffer_one.clear();
                    }
                }
            }else{
                if(!buffer_two.contains(identifiedSign)){
                    buffer_two.putIfAbsent(identifiedSign, new ArrayList<List<Object>>());
                }
                buffer_two.get(identifiedSign).add(data);
                LOG.debug("count:"+count);
                if(count.getAndIncrement()%maxBuffered==0){
                    LOG.info("mark changed to "+mark.incrementAndGet());
                    synchronized (buffer_two) {
                        LOG.info(workName +"two:" +" contains "+buffer_two.size()+" targets to send...");
                        this.deliver(buffer_two);
                        buffer_two.clear();
                    }
                }
            }
        }else{//处理flush/commit操作
            if("flush".equals(event.getOpera())){
                LOG.info(workName +"one:flush"+buffer_one.size());
                LOG.info(workName +"two:flush"+buffer_two.size());
                deliver(buffer_one);
                deliver(buffer_two);
                buffer_one.clear();
                buffer_two.clear();
                synchronized (coordinator) {
                    coordinator.get(workName).getAndIncrement();
                }
                boolean allChecked = false;
                long waitingtimes = 0;
                while(true){
                    for(String worker : event.getOperators()){
                        if(coordinator.get(workName).get() != coordinator.get(worker).get()){
                            waitingtimes+=50;
                            Thread.sleep(50);
                            allChecked = false;
                            break;
                        }else{
                            allChecked = true;
                        }
                    }
                    if(allChecked){
                        LOG.info("thread "+ workName + " total waited " + waitingtimes + " ms");
                        LOG.info("total added---->>  " + totalAdded);
                        LOG.info("total sent------->>  " + totalSent);
                        break;
                    }else{
                        continue;
                    }
                }
            }else{
//                return;
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
                totalSent.getAndAdd(dataSet.size());
                dataContent = dataAssist.serialize(SchemaManager.getSchema(keys[0]+keys[1]), dataSet);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            try {
                ConnectionCollection.getAvroCollection(getHost(keys[0],keys[1],keys[2]), ClientCache.HTTP_SOURCE_PORT).flush(heads, dataContent);
//                ConnectionCollection.getThriftCollection(getHost(keys[0],keys[1],keys[2]), ClientCache.HTTP_SOURCE_PORT).flush(heads, dataContent);
//                ConnectionCollection.getHTTPCollection(getHost(keys[0],keys[1],keys[2]), ClientCache.HTTP_SOURCE_PORT).flush(heads, dataContent);
                LOG.info("");
            }
            catch (Exception e) {
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