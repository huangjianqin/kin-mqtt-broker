package org.kin.mqtt.broker.bridge.definition;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;

import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2023/5/26
 */
public abstract class AbstractBridgeDefinition implements BridgeDefinition{
    private static final long serialVersionUID = 8711308396094343662L;
    /** bridge name */
    private String name;

    @Override
    public void check() {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "bridge name must not blank");
    }

    /** builder **/
    public static abstract class Builder<BD extends AbstractBridgeDefinition, BDB extends Builder<BD, BDB>> {
        protected BD definition;

        protected Builder(BD definition) {
            this.definition = definition;
        }

        @SuppressWarnings("unchecked")
        public BDB name(String name) {
            definition.setName(name);
            return (BDB) this;
        }

        public BD build() {
            definition.check();
            return definition;
        }
    }

    //setter && getter
    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractBridgeDefinition that = (AbstractBridgeDefinition) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "name='" + name + '\'';
    }
}
