package org.kin.mqtt.broker.rule.action.impl.mqtt;

import com.google.common.base.Preconditions;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.utils.Extension;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.AbstractActionFactory;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

import static org.kin.mqtt.broker.rule.action.impl.mqtt.MqttTopicActionConstants.QOS_KEY;
import static org.kin.mqtt.broker.rule.action.impl.mqtt.MqttTopicActionConstants.TOPIC_KEY;

/**
 * @author huangjianqin
 * @date 2023/9/24
 */
@Extension(value = "mqttTopic")
public class MqttTopicActionFactory extends AbstractActionFactory<MqttTopicAction> {
    @Override
    protected void checkConfig(ActionConfiguration config) {
        super.checkConfig(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(config.get(TOPIC_KEY)), "mqtt topic must be not blank");
        MqttQoS.valueOf(config.get(QOS_KEY));
    }

    @Override
    protected MqttTopicAction create0(ActionConfiguration config) {
        return new MqttTopicAction(config);
    }
}
