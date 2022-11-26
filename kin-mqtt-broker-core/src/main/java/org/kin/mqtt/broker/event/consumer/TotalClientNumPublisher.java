package org.kin.mqtt.broker.event.consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.event.EventFunction;
import org.kin.framework.event.EventListener;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.TopicNames;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.VirtualMqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.event.MqttClientConnEvent;
import org.kin.mqtt.broker.event.MqttClientDisConnEvent;

/**
 * @author huangjianqin
 * @date 2022/11/26
 */
@EventListener
public final class TotalClientNumPublisher {
    @EventFunction
    public void onMqttClientConn(MqttClientConnEvent event) {
        publishTotalClientNum(event.getMqttChannel());
    }

    @EventFunction
    public void onMqttClientDisConn(MqttClientDisConnEvent event) {
        publishTotalClientNum(event.getMqttChannel());
    }

    /**
     * 往{@link  TopicNames##SYS_TOPIC_CLIENTS_TOTAL}系统topic publish消息
     */
    private void publishTotalClientNum(MqttChannel mqttChannel) {
        MqttBrokerContext brokerContext = mqttChannel.getBrokerContext();
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        buffer.writeBytes(JSON.writeBytes(brokerContext.getChannelManager().size()));
        MqttPublishMessage message = MqttMessageUtils.createPublish(false, MqttQoS.AT_MOST_ONCE,
                false, 0,
                TopicNames.SYS_TOPIC_CLIENTS_TOTAL,
                buffer);
        brokerContext.getDispatcher().dispatch(MqttMessageWrapper.common(message),
                new VirtualMqttChannel(brokerContext, brokerContext.getBrokerId()), brokerContext);
    }
}
