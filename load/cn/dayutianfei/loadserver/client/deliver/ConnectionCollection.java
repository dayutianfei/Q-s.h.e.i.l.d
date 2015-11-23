package cn.dayutianfei.loadserver.client.deliver;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 加载服务端连接池
 *
 * @author wzy
 * @date 2015-8-22 14:19:24
 */
public class ConnectionCollection {
	
	private static Logger LOG = LoggerFactory.getLogger(ConnectionCollection.class);
	
	//TODO 连接数量过多会导致缓存的数据过大
	private static ConcurrentMap<String, Connection> connectionCache = new ConcurrentHashMap<String, Connection>();

	public static void ini(){
	}
	
	public static Connection getHTTPCollection(String host, int port) {
	    String dest = host+":"+port;
//	    return  new HttpConnection(host, port); 
	    LOG.debug("get the connect for" + dest);
	    if(!connectionCache.containsKey(dest)){
	        Connection connection = new HttpConnection(host, port);
	        connectionCache.putIfAbsent(dest,connection);
	        return connection;
	    }
		return connectionCache.get(dest);
	}
	
	public static Connection getAvroCollection(String host, int port) {
        String dest = host+":"+port;
//      return  new HttpConnection(host, port); 
        LOG.debug("get the connect for" + dest);
        if(!connectionCache.containsKey(dest)){
            Connection connection = new AvroConnection(host, port);
            try {
                connection.open();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            connectionCache.putIfAbsent(dest,connection);
            return connection;
        }
        return connectionCache.get(dest);
    }
	
	public static Connection getThriftCollection(String host, int port) {
        String dest = host+":"+port;
//      return  new HttpConnection(host, port); 
        LOG.debug("get the connect for" + dest);
        if(!connectionCache.containsKey(dest)){
            Connection connection = new ThriftConnection(host, port);
            try {
                connection.open();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            connectionCache.putIfAbsent(dest,connection);
            return connection;
        }
        return connectionCache.get(dest);
    }
}
