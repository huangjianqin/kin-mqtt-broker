package org.kin.mqtt.broker.store;

/**
 * @author huangjianqin
 * @date 2022/11/16
 */
public abstract class AbstractMqttMessageStore implements MqttMessageStore {
    @Override
    public void close() {
        //默认do nothing
    }
}
