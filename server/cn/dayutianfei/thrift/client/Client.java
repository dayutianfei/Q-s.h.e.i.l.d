package cn.dayutianfei.thrift.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import cn.dayutianfei.thrift.api.ThriftService;
 
  public class Client {
 
     public void startClient() {
         TTransport transport;
         try {
             transport = new TSocket("127.0.0.1", 9090);
             TProtocol protocol = new TBinaryProtocol(transport);
             ThriftService.Client client = new ThriftService.Client(protocol);
             transport.open();
             System.out.println(client.demoMethod("tianfei x"));
             transport.close();
         } catch (TTransportException e) {
             e.printStackTrace();
         } catch (TException e) {
             e.printStackTrace();
         }
     }
 
     public static void main(String[] args) {
         Client client = new Client();
         client.startClient();
     }
 }
