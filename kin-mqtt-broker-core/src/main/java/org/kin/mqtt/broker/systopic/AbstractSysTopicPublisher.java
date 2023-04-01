package org.kin.mqtt.broker.systopic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.VirtualMqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;

import java.util.Map;

/**
 * publish 系统级别消息的统一父类
 *
 * @author huangjianqin
 * @date 2022/11/27
 */
public abstract class AbstractSysTopicPublisher {
    /**
     * publish 系统级别消息
     *
     * @param brokerContext mqtt broker context
     * @param data          消息数据
     */
    protected void publishSysMessage(MqttBrokerContext brokerContext, String topic, Map<String, Object> data) {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        buffer.writeBytes(JSON.writeBytes(data));
        MqttPublishMessage message = MqttMessageUtils.createPublish(false, MqttQoS.AT_MOST_ONCE,
                true, 0,
                topic,
                buffer);
        brokerContext.getDispatcher().dispatch(MqttMessageWrapper.common(message),
                new VirtualMqttSession(brokerContext, brokerContext.getBrokerClientId()), brokerContext);
    }
}
