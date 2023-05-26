package org.kin.mqtt.broker.bridge.http.boot;

import org.kin.mqtt.broker.bridge.Bridge;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/11/22
 */
@ConditionalOnClass(Bridge.class)
@Configuration
public class HttpBridgeAutoConfiguration {
    /**
     * 默认只加载default的http bridge
     */
    @Bean
    public Bridge httpBridge(@Autowired(required = false) WebClient webClient) {
        String name = "_default_http_bridge";
        if (Objects.nonNull(webClient)) {
            return new HttpBridge(name, webClient);
        }
        return new HttpBridge(name);
    }
}
