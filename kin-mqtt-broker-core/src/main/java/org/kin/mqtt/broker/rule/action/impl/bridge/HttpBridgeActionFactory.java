package org.kin.mqtt.broker.rule.action.impl.bridge;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.Extension;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

/**
 * @author huangjianqin
 * @date 2023/9/24
 */
@Extension(value = "httpBridge")
public class HttpBridgeActionFactory extends BridgeActionFactory<HttpBridgeAction> {
    @Override
    protected void checkConfig(ActionConfiguration config) {
        super.checkConfig(config);
        Preconditions.checkArgument(StringUtils.isNotBlank(config.get(BridgeActionConstants.HTTP_URI_KEY)), "http uri must be not blank");
    }

    @Override
    protected HttpBridgeAction create0(ActionConfiguration config) {
        return new HttpBridgeAction(config);
    }
}
