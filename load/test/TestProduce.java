package test;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.client.BalancedClient;
import cn.dayutianfei.loadserver.client.Client;

public class TestProduce {
    private static Logger LOG=LoggerFactory.getLogger(TestProduce.class);
    public static void main(String[] args) throws Exception {
//        Client client = new BalancedClient("172.16.2.203",5140,30);
        Client client = new BalancedClient("162.16.2.65",-1,30);
        System.out.println("begin to send msg");
        long one = System.currentTimeMillis();
        long totalSize = 0;
        for(int i = 0; i < 10000000 ; i++){
            List<Object> data = new ArrayList<Object>();
            data.add(i);
            String _data1= "xxxxxxxx";
            data.add(_data1);
            String xxxx = "xxxxxxx "+i;
            data.add(xxxx);
            String dt = (100000000000l+i)+"";
            data.add(dt);
            String url = "www..baidu.com/xxx/abc/123/xxx/xxx"+i;
            data.add(url);
            data.add(url);data.add(url);data.add(url);data.add(url);data.add(url);
            data.add(url);data.add(url);data.add(url);data.add(url);data.add(url);
            data.add(url);data.add(url);data.add(url);data.add(url);data.add(url);
            client.insert("test", "test1", data);
            totalSize+= (4+_data1.getBytes().length+xxxx.getBytes().length+dt.getBytes().length+url.getBytes().length*16);
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
        client.close();
        
    }
}
