package cn.dayutianfei.loadserver.client;

import java.util.List;

/**
 * 客户端抽象类
 *
 * @author wzy
 * @date 2015-8-21 14:08:58
 */
public abstract class Client {
    
    public String host = null;  // 加载初始化时接入的主机名或IP
    public int hostPort = -1;   // 端口号
    public int timeoutInSeconds = 0; // 后端交互的超时时间，单位为秒
    
    /**
     * 初始化构造方法
     * @param hostNameOrIp  加载初始化时接入的主机名或IP
     * @param port                        端口号
     * @param timeoutInSecond   后端交互的超时时间，单位为秒
     */
    public Client(String hostNameOrIp, int port, int timeoutInSecond){
        host = hostNameOrIp;
        hostPort = port;
        timeoutInSeconds = timeoutInSecond;
    }

	/**
	 * 单条数据的加载
	 * @param dbName //数据库名，不区分大小写
	 * @param tblName //表名，不区分大小写
	 * @param data         //待插入的数据
	 */
	public abstract boolean insert(String dbName, String tblName,List<Object> data)throws Exception;

	/**
	 * 数据提交（此处会保证事务）
	 */
	public abstract void commit() throws Exception ;

	/**
	 * 发送当前缓存数据到后端
	 */
	public abstract void flush() throws Exception;
	
	/**
	 * 关闭客户端
	 */
	public abstract void close() throws Exception;
	
	/**
	 * 重新加载配置文件和元数据信息
	 * @param host 主机名或IP
	 * @param port 主机对应的端口号
	 * @param timeoutInSeconds 连接的超时时间，单位：秒
	 */
	public abstract void reloadConf(String host, int port, int timeoutInSeconds) throws Exception;
}
