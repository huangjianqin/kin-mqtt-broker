package org.kin.mqtt.broker.rule.action.bridge.definition;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.ActionDefinition;

import java.util.Objects;

/**
 * 桥接动作规则抽象定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 */
public abstract class BridgeActionDefinition implements ActionDefinition {
    /** bridge name */
    private String bridgeName;

    /** builder **/
    public static abstract class Builder<BRD extends BridgeActionDefinition, B extends Builder<BRD, B>> {
        protected BRD definition;

        protected Builder(BRD definition) {
            this.definition = definition;
        }

        @SuppressWarnings("unchecked")
        public B bridgeName(String bridgeName) {
            definition.setBridgeName(bridgeName);
            return (B) this;
        }

        public BRD build() {
            Preconditions.checkArgument(StringUtils.isNotBlank(definition.getBridgeName()), "bridge name must be not blank");
            return definition;
        }
    }

    //setter && getter
    public String getBridgeName() {
        return bridgeName;
    }

    public void setBridgeName(String bridgeName) {
        this.bridgeName = bridgeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BridgeActionDefinition)) {
            return false;
        }
        BridgeActionDefinition that = (BridgeActionDefinition) o;
        return Objects.equals(bridgeName, that.bridgeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bridgeName);
    }

    @Override
    public String toString() {
        return "bridgeName='" + bridgeName + "', ";
    }
}
