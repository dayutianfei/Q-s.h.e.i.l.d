package cn.dayutianfei.zookeeper.monitor;

import org.I0Itec.zkclient.ZkClient;

public class ZKFileMonitor {

    private static ZkClient _zkClient = null;
    /**
     * @param args
     */
    public static void main(String[] args) {
        if(null == args || args.length != 3){
            System.out.println("input the zk address like '172.16.2.202:2181,172.16.2.203:2181'");
            System.out.println("input the zk root dir like '/iDriller/loadservers'");
            System.out.println("input the key word end with , like '.parq'");
            System.out.println("the application will show you the number of child nodes in the given root");
            return;
        }
//        String[] test = new String[]{"127.0.0.1:2182","/test",".parq"};
//        args = test;
        _zkClient = new ZkClient(args[0],30*1000,30*1000);
//        _zkClient.createPersistent("/test");
//        _zkClient.createPersistent("/test/test5.rc");
//        _zkClient.createPersistent("/test/test5.rc/test1");
//        _zkClient.createPersistent("/test/test4/test1/test1.rc");
//        _zkClient.createPersistent("/test/test3/test.rc");
        int livingMark = 0;
        int sleepingInterval = 7200*1000;
        while(true){
            livingMark++;
            try{
                if(null == _zkClient){
                    _zkClient = new ZkClient(args[0],30*1000,30*1000);
                    System.out.println("error that the client becomes null on " + System.currentTimeMillis());
                }
                long number = count(args[1],_zkClient,args[2]);
                System.out.println("the root dir on zk : "+ args[1]);
                System.out.println("has about : " + number + " child nodes");
                Thread.sleep(sleepingInterval);
                System.out.println("the app live for : "+ livingMark*sleepingInterval + " ms");
            }catch(Exception e){
                System.out.println("error: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
    }
    
    public static long count(String root, ZkClient zk, String suffix){
        if(zk.countChildren(root)==0 || root.endsWith(suffix)){
            if(root.endsWith(suffix)){
                return 1;
            }else{
                return 0;
            }         
        }else{
            long temp = 0;
            for(String child: zk.getChildren(root)){
                temp +=count(root+"/"+child,zk,suffix);
            }
            return temp;
        }
    }

}
