package cn.dayutianfei.rpc.thrift;

import cn.dayutianfei.rpc.thrift.server.ThriftTCPServer;
import cn.dayutianfei.rpc.thrift.server.ThriftTTPServer;

public class ThriftServerMain {
    public static void main(String[] args) {
        ThriftTTPServer server = new ThriftTTPServer();
        server.ini();
        ThriftTCPServer server1 = new ThriftTCPServer();
        server1.ini();
        try {
            server1.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
