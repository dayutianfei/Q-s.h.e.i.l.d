package cn.dayutianfei.rpc.thrift.server;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import cn.dayutianfei.loadserver.lib.thrift.protocol.ThriftFlumeEvent;
import cn.dayutianfei.loadserver.lib.thrift.protocol.ThriftSourceProtocol.Client;

public class ThriftTTPClient {

	private TFastFramedTransport transport;
	private Client client;
	private String hostName = null;
	private int hostPort = -1;

	public ThriftTTPClient(String host, int port) {
		this.hostName = host;
		this.hostPort = port;
		this.transport = new TFastFramedTransport(new TSocket(hostName, hostPort));
        this.client = new Client(new TCompactProtocol(transport));
	}

	public void open() throws IOException {
		try {
			transport.open();
		} catch (TTransportException e) {
			throw new IOException("Open error.", e);
		}
	}

	public boolean isActive() {
		if (transport != null) {
			return transport.isOpen();
		}
		return false;
	}

    public void flush(HashMap<String, String> heads, byte[] data) throws IOException {
        if (!this.isActive()) {
//            throw new IOException("Not open.");
            this.open();
        }
        try {
            ThriftFlumeEvent thriftEvent = new ThriftFlumeEvent();
            thriftEvent.setHeaders(heads);
            thriftEvent.setBody(data);
            String returned =  client.append(thriftEvent).toString();
            Log.info("the message returned is "+returned);
        } catch (TException e) {
            throw new IOException("Append error.", e);
        }
        
    }

    public boolean commit(HashMap<String, String> heads) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }
    
    public void close() {
        if (transport != null) {
            transport.close();
            transport = null;
            client = null;
        }
    }
}
