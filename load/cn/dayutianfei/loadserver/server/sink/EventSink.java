package cn.dayutianfei.loadserver.server.sink;

import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.client.cache.ClientCache;
import cn.dayutianfei.loadserver.client.message.DataMessage;
import cn.dayutianfei.loadserver.common.Codes;
import cn.dayutianfei.loadserver.lib.serde.DataDeserialization;

/**
 * @ClassName:	EventSink 
 * @Description:flume sink  
 * @author:	hzb
 * @date:	2015年6月29日 下午1:23:05 
 *
 */
public class EventSink extends AbstractSink implements Configurable {
	
	private static final Logger LOG = LoggerFactory.getLogger(EventSink.class);
	
	private long totalDealNumber = 0;
	private DataDeserialization dataAssist = null;

    @Override
    public void configure(Context arg0) {
        ClientCache.updateCache();
        dataAssist = new DataDeserialization();
        LOG.info("done configure the sink : " + getName());
    }
    
	@Override
    public Status process() throws EventDeliveryException {
        LOG.info("the sink : " + getName()+" start to process file");
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            if (event == null) {
                transaction.commit();
                return Status.BACKOFF;
            }
            String dbName = event.getHeaders().get(DataMessage.HEAD_DB);
            String tblName = event.getHeaders().get(DataMessage.HEAD_TBL);
            String event_code = event.getHeaders().get(DataMessage.HEAD_OP);
            if (dbName == null || tblName == null) {
                LOG.error("DbName or TblName is null: " + dbName + "@" + tblName);
                transaction.commit();
                return Status.BACKOFF;
            }
            if (Codes.FLUSH_DATA.equals(event_code)) {
//                // TODO:关闭HDFS写入对象
//                int num = dataAssist.deser(dbName, tblName, event.getBody()).size();
//                totalDealNumber += num;
//                LOG.info("insert data number " + num + ", total is " + totalDealNumber);
//                transaction.commit();
//                return Status.READY;
            }
            // TODO:写入HDFS
            List<List<Object>> lines = dataAssist.deser(dbName, tblName, event.getBody());
            int num = lines.size();
            totalDealNumber += num;
            LOG.info("insert data number " + num + ", total is " + totalDealNumber);
            transaction.commit();
            return Status.READY;
        }
        catch (Exception e) {
            LOG.error("process cause error: ", e);
            transaction.rollback();
            return Status.BACKOFF;
        }
        finally {
            transaction.close();
        }
    }

	@Override
	public void start() {
		super.start();
		LOG.info("EventSink " + getName() + " startted.");
	}

	@Override
	public void stop() {
		super.stop();
		LOG.info("EventSink " + getName() + " stopped.");
	}
	
}
