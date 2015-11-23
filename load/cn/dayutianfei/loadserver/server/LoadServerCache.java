package cn.dayutianfei.loadserver.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.server.transport.HTTPTransport;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class LoadServerCache {
    
    public enum OperaType {
        ADDHOST, REMOVEHOST, SHOWHOSTS
    }
    
    private static final Logger LOG = LoggerFactory.getLogger(LoadServerCache.class);
    
    public static Cache<String, String> cache = CacheBuilder.newBuilder()  
            .maximumSize(1000).build();
    
    public static List<String> hostName = new ArrayList<String>();
    
    /**
     * 增加本地缓存的主机列表
     * 如果添加成功，则将本地的缓存信息发给该主机
     * @param hostNameOrIP
     */
    public static void addBackendHost(String hostNameOrIP){
        if(!hostName.contains(hostNameOrIP)){
            cache.put(hostNameOrIP, hostNameOrIP);
            hostName.add(hostNameOrIP);
            for(String host: hostName){
                if(host.equals(hostNameOrIP)){
                    continue;
                }
                try {
                    HTTPTransport.send(OperaType.ADDHOST.toString() ,host);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            LOG.info("host : " + hostNameOrIP + " added success");
        }
    }
    
    public static void removeBackendHost(String hostNameOrIP){
        if(hostName.contains(hostNameOrIP)){
            hostName.remove(hostNameOrIP);
            for(String host: hostName){
                try {
                    HTTPTransport.send(OperaType.REMOVEHOST.toString(), host);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            LOG.info("host : " + hostNameOrIP + " removed success");
        }
    }
}
