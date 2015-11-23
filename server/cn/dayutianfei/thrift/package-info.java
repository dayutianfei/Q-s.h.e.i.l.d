/**
 * @author dayutianfei
 * 主要用于处理Thrift相关的Server，以提供多语言接口能力
 * ThriftServer主要包括：
 * 1.SimpleServer: 单线程，每个线程阻塞
 * 2.ThreadServer: 多线程，来一个连接开一个线程，线程阻塞。
 * 3.ThreadPoolServer: 线程池，每个线程阻塞
 * 4.NonBlockingServer: 线程池，非阻塞。用epoll模型实现。用起来很方便。
 * 5.ThreadedSelectorServer: 
 * 6.HsHaServer: 半同步半异步的服务端模型
 */
package cn.dayutianfei.thrift;