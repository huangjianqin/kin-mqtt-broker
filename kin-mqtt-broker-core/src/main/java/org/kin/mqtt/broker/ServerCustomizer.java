package org.kin.mqtt.broker;

import io.netty.channel.ChannelOption;

import java.util.Collections;
import java.util.Map;

/**
 * @author huangjianqin
 * @date 2023/6/18
 */
public interface ServerCustomizer {
    /** 自定义netty options */
    @SuppressWarnings("rawtypes")
    default Map<ChannelOption, Object> options() {
        return Collections.emptyMap();
    }

    /** 自定义netty child options */
    @SuppressWarnings("rawtypes")
    default Map<ChannelOption, Object> childOptions() {
        return Collections.emptyMap();
    }
}
