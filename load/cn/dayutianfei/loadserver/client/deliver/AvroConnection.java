package cn.dayutianfei.loadserver.client.deliver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.lib.avro.protocol.AvroFlumeEvent;
import cn.dayutianfei.loadserver.lib.avro.protocol.AvroSourceProtocol;
import cn.dayutianfei.loadserver.lib.avro.protocol.AvroSourceProtocol.Callback;

public class AvroConnection extends Connection {
    private static Logger LOG=LoggerFactory.getLogger(AvroConnection.class);
	private NettyTransceiver transceiver;
	private Callback client;
	private String hostName = null;
	private int hostPort = -1;

	public AvroConnection(String host, int port) {
		super(host+":"+port);
		this.hostName = host;
		this.hostPort = port;
	}

	@Override
	public void open() throws IOException {
		try {
			transceiver = new NettyTransceiver(new InetSocketAddress(hostName, hostPort));
			client = SpecificRequestor.getClient(AvroSourceProtocol.Callback.class, transceiver);
		} catch (IOException e) {
		    LOG.error(e.getMessage());
			throw new IOException("Open error.", e);
		}
	}

	public String getSchema(String dbName, String tblName) throws IOException {
		if (!isActive()) {
			throw new IOException("Not open.");
		}
		try {
			if (dbName == null) {
				dbName = "";
			}
			if (tblName == null) {
				tblName = "";
			}
			return client.getSchema(dbName, tblName).toString();
		} catch (AvroRemoteException e) {
			throw new IOException("Get schema error.", e);
		}
	}

    @Override
    public void flush(HashMap<String, String> heads, byte[] data) throws IOException {
        if (!isActive()) {
//            throw new IOException("Not open.");
            this.open();
        }
        AvroFlumeEvent avroEvent = new AvroFlumeEvent();
        avroEvent.setHeaders(toCharSeqMap(heads));
        avroEvent.setBody(ByteBuffer.wrap(data));
        try {
            String returned = client.append(avroEvent).toString();
            Log.info("get result status : "+returned);
        } catch (AvroRemoteException e) {
            throw new IOException("Append error.", e);
        }
    }

    @Override
    public boolean commit(HashMap<String, String> heads) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public boolean isActive() {
        return transceiver.isConnected();
    }

    @Override
    public void close() {
        transceiver.close();
    }

    private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
        Map<CharSequence, CharSequence> charSeqMap = new HashMap<CharSequence, CharSequence>();
        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            charSeqMap.put(entry.getKey(), entry.getValue());
        }
        return charSeqMap;
    }
}
