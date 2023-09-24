package org.kin.mqtt.broker.rule.action;

import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2023/9/24
 */
public abstract class AbstractAction implements Action {
    /** 动作配置 */
    protected final ActionConfiguration config;

    protected AbstractAction(ActionConfiguration config) {
        this.config = config;
    }

    @Override
    public final ActionConfiguration configuration() {
        return config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractAction)) {
            return false;
        }
        AbstractAction that = (AbstractAction) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

    @Override
    public String toString() {
        return "config=" + config;
    }
}
