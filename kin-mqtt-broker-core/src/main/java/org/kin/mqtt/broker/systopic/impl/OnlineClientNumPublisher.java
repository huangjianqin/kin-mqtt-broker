package org.kin.mqtt.broker.systopic.impl;

import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.mqtt.broker.TopicNames;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.kin.mqtt.broker.core.event.OnlineClientNumEvent;
import org.kin.mqtt.broker.systopic.AbstractSysTopicPublisher;

import java.util.HashMap;
import java.util.Map;

/**
 * 广播在线mqtt client数量变化mqtt publish消息
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
public final class OnlineClientNumPublisher extends AbstractSysTopicPublisher implements MqttEventConsumer<OnlineClientNumEvent> {
    public OnlineClientNumPublisher(MqttBrokerContext brokerContext) {
        super(brokerContext);
        //注册内部consumer
        brokerContext.getEventBus().register(this);
    }

    @Override
    public void consume(ReactorEventBus eventBus, OnlineClientNumEvent event) {
        Map<String, Object> data = new HashMap<>(1);
        data.put("onlineClients", event.getOnlineNum());

        publishSysMessage(TopicNames.SYS_ONLINE_CLIENTS_TOTAL, data);
    }
}
