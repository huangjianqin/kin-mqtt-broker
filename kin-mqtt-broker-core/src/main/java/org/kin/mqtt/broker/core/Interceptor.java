package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttMessage;
import org.kin.mqtt.broker.core.message.MqttMessageContext;

/**
 * mqtt消息处理流程拦截器
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public interface Interceptor {
    /**
     * mqtt消息处理逻辑
     *
     * @param messageContext mqtt message context
     * @param mqttSession    mqtt session
     * @param context        mqtt broker context
     * @return 是否拦截, 返回true, 后续拦截器将无法继续执行
     */
    boolean intercept(MqttMessageContext<? extends MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext context);

    /**
     * @return 优先级
     */
    int order();
}
