package cn.dayutianfei.loadserver.client.cache.handler.q;

import cn.dayutianfei.loadserver.client.cache.DataEntity;

import com.lmax.disruptor.EventFactory;

/**
 * 定义处理DataEntityEvent的工厂类
 * 事件工厂(Event Factory)定义了如何实例化之前定义的事件(Event)，
 * 需要实现接口 com.lmax.disruptor.EventFactory<T>。
 * Disruptor 通过 EventFactory 在 RingBuffer 中预创建 Event 的实例。
 * @author wzy
 *
 */
public class DataEntityFactory implements EventFactory<DataEntity>{
    public DataEntity newInstance(){
        return new DataEntity();
    }
}
