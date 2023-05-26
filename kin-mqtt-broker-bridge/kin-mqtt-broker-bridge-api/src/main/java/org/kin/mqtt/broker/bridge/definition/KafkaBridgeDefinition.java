package org.kin.mqtt.broker.bridge.definition;

import org.kin.mqtt.broker.core.Type;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka bridge配置定义
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
@Type("kafka")
public class KafkaBridgeDefinition extends AbstractBridgeDefinition{
    private Map<String, Object> props = new HashMap<>();

    public static Builder builder() {
        return new Builder();
    }


    /** builder **/
    public static class Builder extends AbstractBridgeDefinition.Builder<KafkaBridgeDefinition, Builder> {
        protected Builder() {
            super(new KafkaBridgeDefinition());
        }

        public Builder prop(String key, Object value) {
            definition.props.put(key, value);
            return this;
        }

        public Builder props(Map<String, Object> props) {
            definition.props.putAll(props);
            return this;
        }
    }

    //setter && getter
    public Map<String, Object> getProps() {
        return props;
    }

    public void setProps(Map<String, Object> props) {
        this.props = props;
    }

    @Override
    public String toString() {
        return "KafkaBridgeDefinition{" +
                super.toString() +
                ", props=" + props +
                '}';
    }
}
