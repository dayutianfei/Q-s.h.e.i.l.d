package cn.dayutianfei.thrift.server;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import cn.dayutianfei.thrift.api.ThriftService;
import cn.dayutianfei.thrift.api.ThriftService.Iface;
import cn.dayutianfei.thrift.api.ThriftService.Processor;
import cn.dayutianfei.thrift.api.impl.DemoImpl;


/**
 * @author	dayutianfei
 * @date	2013-10-18
 */
public class ThriftThreadPoolServer {
	public static void main(String[] args){
		TServerSocket serverTransport = null;
		try {
			serverTransport = new TServerSocket(9090);
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		ThriftService.Processor<Iface> process = new Processor<Iface>(new DemoImpl());

        Factory portFactory = new TBinaryProtocol.Factory(true, true);

        Args threadPoolServerArgs = new Args(serverTransport);
        threadPoolServerArgs.processor(process);
        threadPoolServerArgs.protocolFactory(portFactory);

        TServer server = new TThreadPoolServer(threadPoolServerArgs);
        server.serve();
	}
}

