package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * disconnect消息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class DisconnectHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @SuppressWarnings("unchecked")
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        //如果网络连接关闭时(DISCONNECT 报文中的 Session Expiry Interval 可以覆盖 CONNECT 报文中的设置), Session Expiry Interval 大于0, 则客户端与服务端必须存储会话状态
        MqttMessage disconnectMessage = wrapper.getMessage();
        MqttReasonCodeAndPropertiesVariableHeader headers = (MqttReasonCodeAndPropertiesVariableHeader) disconnectMessage.variableHeader();
        MqttProperties properties = headers.properties();
        MqttProperties.MqttProperty<Integer> sessionExpiryIntervalProp = properties.getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
        if (Objects.nonNull(sessionExpiryIntervalProp)) {
            mqttChannel.updateSessionExpiryInterval(sessionExpiryIntervalProp.value());
        }
        return mqttChannel.close();
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.DISCONNECT;
    }
}
