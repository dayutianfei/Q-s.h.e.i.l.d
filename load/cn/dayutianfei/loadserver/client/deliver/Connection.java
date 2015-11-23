package cn.dayutianfei.loadserver.client.deliver;

import java.io.IOException;
import java.util.HashMap;

/**
 * 目标服务端连接对象
 * @author wzy
 * @date 2015-8-22 13:32:31
 */
public abstract class Connection {
	//连接目标的标识
	protected final String dest;

	/**
	 * 构造方法
	 * @param host 使用主机+冒号+端口的形式，如：host01:20202
	 */
	public Connection(String host) {
		this.dest = host;
	}

	/**
	 * 打开连接，保持长会话状态
	 * @throws IOException
	 */
	public abstract void open() throws IOException;
	
	/**
	 * 向目标节点发送数据
	 * @param heads    消息头，使用键值对形式表示
	 * @param data       消息数据域，使用符合要求的二进制数组表示
	 */
	public abstract void flush(HashMap<String,String> heads, byte[] data) throws IOException;
	
	/**
	 * 强制服务端提交发送的数据，即写盘
	 * @param heads    消息头，使用键值对形式表示
	 */
	public abstract boolean commit(HashMap<String,String> heads) throws IOException;

	/**
	 * 当前连接是否有效
	 * @return true 有效；false 无效或已关闭或异常
	 */
	public abstract boolean isActive();

	/**
	 * 关闭当前连接
	 */
	public abstract void close();

}
