package org.kin.mqtt.broker.bridge.definition;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.kin.mqtt.broker.bridge.Bridge;

import java.io.Serializable;

/**
 * {@link Bridge}实现定义
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface BridgeDefinition extends Serializable {
    /**
     * 检查配置
     */
    default void check() {
        //default do nothing
    }

    /**
     * 获取bridge name
     * @return bridge name
     */
    String getName();
}
