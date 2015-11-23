package cn.dayutianfei.zookeeper.monitor;

import org.I0Itec.zkclient.ZkClient;

public class ZKMonitor {

    private static ZkClient _zkClient = null;
    /**
     * @param args
     */
    public static void main(String[] args) {
        _zkClient = new ZkClient("172.16.2.202:2181,172.16.2.203:2181,172.16.2.206:2181",30*1000,30*1000);
        int livingMark = 0;
        int sleepingInterval = 30*1000;
        while(true){
            livingMark++;
            try{
                if(null == _zkClient){
                    _zkClient = new ZkClient("172.16.2.202:2181,172.16.2.203:2181,172.16.2.206:2181",30*1000,30*1000);
                    System.out.println("error that the client becomes null on " + System.currentTimeMillis());
                }
                int serverNumber = _zkClient.countChildren("/iDriller/loadservers");
                if(serverNumber != 4){
                    System.out.println("error: the server number is "+ serverNumber + " on " + System.currentTimeMillis());
                }
                if(livingMark % 20 == 0){
                    System.out.println("the zk state check thread is working for about : " + ( livingMark*sleepingInterval /1000 ) + "s");
                }
                Thread.sleep(sleepingInterval);
            }catch(Exception e){
                System.out.println("error: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
    }

}
