package org.kin.mqtt.broker.rule.definition;

/**
 * 桥接动作规则抽象定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 */
public abstract class BridgeActionDefinition extends RuleDefinition {
    /** bridge name */
    private String bridgeName;

    /** builder **/
    public static abstract class Builder<BRD extends BridgeActionDefinition> extends RuleDefinition.Builder<BRD> {
        protected Builder(BRD definition) {
            super(definition);
        }

        @SuppressWarnings("unchecked")
        public <B extends Builder<BRD>> B bridgeName(String bridgeName) {
            definition.setBridgeName(bridgeName);
            return (B) this;
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
    public String toString() {
        return super.toString() +
                "bridgeName='" + bridgeName + "', ";
    }
}
