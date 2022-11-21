package org.kin.mqtt.broker.store;

/**
 * @author huangjianqin
 * @date 2022/11/16
 */
public abstract class AbstractMessageStore implements MqttMessageStore {
    @Override
    public void dispose() {
        //默认do nothing
    }
}
