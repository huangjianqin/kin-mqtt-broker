package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import org.kin.mqtt.broker.core.message.MqttMessageHandler;

/**
 * @author huangjianqin
 * @date 2022/11/14
 */
abstract class AbstractMqttMessageHandler<M extends MqttMessage> implements MqttMessageHandler<M> {

}

