package org.kin.mqtt.broker.systopic;

import org.kin.framework.event.EventFunction;
import org.kin.framework.event.EventListener;
import org.kin.mqtt.broker.TopicNames;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.event.MqttClientRegisterEvent;
import org.kin.mqtt.broker.event.MqttClientUnregisterEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huangjianqin
 * @date 2022/11/26
 */
@EventListener
public final class TotalClientNumPublisher extends AbstractSysTopicPublisher {
    @EventFunction
    public void onMqttClientRegister(MqttClientRegisterEvent event) {
        publishTotalClientNum(event.getMqttChannel());
    }

    @EventFunction
    public void onMqttClientUnregister(MqttClientUnregisterEvent event) {
        publishTotalClientNum(event.getMqttChannel());
    }

    /**
     * 往{@link  TopicNames##SYS_TOPIC_CLIENTS_TOTAL}系统topic publish消息
     */
    private void publishTotalClientNum(MqttChannel mqttChannel) {
        MqttBrokerContext brokerContext = mqttChannel.getBrokerContext();

        Map<String, Object> data = new HashMap<>(1);
        data.put("totalClients", brokerContext.getChannelManager().size());

        publishSysMessage(brokerContext, TopicNames.SYS_TOPIC_CLIENTS_TOTAL, data);
    }
}
