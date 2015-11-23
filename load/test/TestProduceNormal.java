package test;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.client.BalancedClient;
import cn.dayutianfei.loadserver.client.Client;

public class TestProduceNormal {
    private static Logger LOG=LoggerFactory.getLogger(TestProduceNormal.class);
    public static void main(String[] args) throws Exception {
//        Client client = new BalancedClient("172.16.2.203",5140,30);
        Client client = new BalancedClient("172.16.2.206",-1,30);
        System.out.println("begin to send msg");
        long one = System.currentTimeMillis();
        long totalSize = 0;
        for(int i = 0; i < 10000000 ; i++){
            List<Object> oridata = new ArrayList<Object>();
            oridata.add(3000);
            oridata.add(114);
            oridata.add(201);
            oridata.add(202);
            oridata.add(203L);
            oridata.add("938.88");
            oridata.add("23157.666");
            oridata.add("xxx");
            oridata.add("xxxx");
            oridata.add("xxxxx");
            oridata.add("xxxxxx");
            oridata.add("http://www.yahoo.com");
            oridata.add("http://www.yahoo.com");
            oridata.add("10.127.68.48");
            oridata.add("2001:1:4137:9e76:1011:290f:53ef:ed63");
            oridata.add("xxxxxx");
            oridata.add("www.yahoo.com".getBytes());
            oridata.add("www.yahoo.com");
            oridata.add("www.yahoo.com");
            oridata.add("20130930000000");
            oridata.add(199);

            client.insert("test", "normal", oridata);
            //totalSize+= (4+_data1.getBytes().length+xxxx.getBytes().length+dt.getBytes().length+url.getBytes().length*16);
        }
        client.flush();
        //System.exit(-1);
        
//        for(int i = 0; i < 10 ; i++){
//            List<Object> data = new ArrayList<Object>();
//            data.add(i);
//            String _data1= "xxxxxxxx";
//            data.add(_data1);
//            String xxxx = "xxxxxxx"+i;
//            data.add(xxxx);
//            String dt = (100000000000l+i)+"";
//            data.add(dt);
//            String url = "www.baidu.com/xxx/xxx/xxx/xxx/xxx"+i;
//            data.add(url);
//            client.insert("test", "test6", data);
//            totalSize+= (4+_data1.getBytes().length+xxxx.getBytes().length+dt.getBytes().length+url.getBytes().length);
//        }
//        client.flush();
//        client.flush();
        long totaltime = System.currentTimeMillis()-one;
        LOG.info("the total loading time is "+totaltime);
        LOG.info("the total size is "+totalSize);
        LOG.info("speed is "+ (totalSize/1024.0/1024.0/totaltime*1000.0)+"MB/s");
       // client.close();
        
    }
}
