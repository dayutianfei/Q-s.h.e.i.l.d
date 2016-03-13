package cn.dayutianfei.rpc.thrift;

import java.io.IOException;
import java.util.HashMap;

import cn.dayutianfei.rpc.thrift.server.ThriftTTPClient;

public class ThriftClientMain {
    public static void main(String[] args) throws IOException {
        String host = "localhost";
        if(null == args || args.length==0){
            // do nothing
        }else{
            host = args[0];
        }
        ThriftTTPClient client = new ThriftTTPClient(host, 7677);
        HashMap<String,String> params = new HashMap<String, String>();
        for(int i=0;i<10000;i++){
            client.flush(params, new byte[100000+1000/(i+1)]);
        }
    }
}
