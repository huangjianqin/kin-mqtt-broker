package org.kin.mqtt.broker.rule.action.impl.bridge;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.Extension;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

/**
 * @author huangjianqin
 * @date 2023/9/24
 */
@Extension(value = "httpKafka")
public class KafkaBridgeActionFactory extends BridgeActionFactory<KafkaBridgeAction> {
    @Override
    protected void checkConfig(ActionConfiguration config) {
        super.checkConfig(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(config.get(BridgeActionConstants.KAFKA_TOPIC_KEY)), "kafka topic must be not blank");
    }

    @Override
    protected KafkaBridgeAction create0(ActionConfiguration config) {
        return new KafkaBridgeAction(config);
    }
}
