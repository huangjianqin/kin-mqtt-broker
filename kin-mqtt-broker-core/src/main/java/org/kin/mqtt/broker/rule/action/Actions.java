package org.kin.mqtt.broker.rule.action;

import org.kin.framework.utils.ExtensionLoader;

import java.util.Objects;

/**
 * {@link Action}工具类
 *
 * @author huangjianqin
 * @date 2022/12/16
 */
public class Actions {
    /**
     * 创建{@link Action}实例
     *
     * @return {@link Action}实例
     */
    @SuppressWarnings("unchecked")
    public static Action createAction(ActionConfiguration config) {
        String type = config.getType();
        ActionFactory<Action> factory = ExtensionLoader.getExtension(ActionFactory.class, type);
        if (Objects.isNull(factory)) {
            throw new IllegalStateException(String.format("can not find action factory named '%s'", type));
        }

        return factory.create(config);
    }
}
