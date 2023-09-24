package org.kin.mqtt.broker.rule.action.impl.bridge;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.AbstractActionFactory;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

/**
 * @author huangjianqin
 * @date 2023/9/24
 */
public abstract class BridgeActionFactory<BA extends BridgeAction> extends AbstractActionFactory<BA> {
    @Override
    protected void checkConfig(ActionConfiguration config) {
        super.checkConfig(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(config.get(BridgeActionConstants.BRIDGE_KEY)), "bridge name must not blank");
    }
}
