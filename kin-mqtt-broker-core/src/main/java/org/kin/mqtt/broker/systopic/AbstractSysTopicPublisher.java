package org.kin.mqtt.broker.systopic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;

import java.util.Map;

/**
 * publish 系统级别消息的统一父类
 *
 * @author huangjianqin
 * @date 2022/11/27
 */
public abstract class AbstractSysTopicPublisher {
    /** mqtt broker context */
    protected final MqttBrokerContext brokerContext;

    protected AbstractSysTopicPublisher(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
    }

    /**
     * publish 系统级别消息
     *
     * @param topic sys topic
     * @param data  消息数据
     */
    protected void publishSysMessage(String topic, Map<String, Object> data) {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        buffer.writeBytes(JSON.writeBytes(data));
        MqttPublishMessage message = MqttMessageHelper.createPublish(false, MqttQoS.AT_MOST_ONCE,
                true, 0,
                topic,
                buffer);
        brokerContext.getDispatcher()
                .dispatch(MqttMessageContext.common(message, brokerContext.getBrokerId(), brokerContext.getBrokerClientId()), brokerContext)
                .subscribe();
    }
}
