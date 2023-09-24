package org.kin.mqtt.broker.rule.action.impl.bridge;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.Extension;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

/**
 * @author huangjianqin
 * @date 2023/9/24
 */
@Extension(value = "rabbitMQBridge")
public class RabbitMQBridgeActionFactory extends BridgeActionFactory<RabbitMQBridgeAction> {
    @Override
    protected void checkConfig(ActionConfiguration config) {
        super.checkConfig(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(config.get(BridgeActionConstants.RABBITMQ_QUEUE_KEY)), "rabbitMQ queue must be not blank");
    }

    @Override
    protected RabbitMQBridgeAction create0(ActionConfiguration config) {
        return new RabbitMQBridgeAction(config);
    }
}
