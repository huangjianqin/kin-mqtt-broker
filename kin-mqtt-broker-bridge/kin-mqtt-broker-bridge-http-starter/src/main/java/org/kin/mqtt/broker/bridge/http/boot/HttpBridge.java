package org.kin.mqtt.broker.bridge.http.boot;

import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.bridge.NoErrorBridge;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * 基于{@link org.springframework.web.reactive.function.client.WebClient}
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class HttpBridge extends NoErrorBridge {
    private final WebClient webClient;

    public HttpBridge() {
        this(WebClient.create());
    }

    public HttpBridge(WebClient webClient) {
        this(BridgeType.HTTP.getDefaultName(), webClient);
    }

    public HttpBridge(String name) {
        this(name, WebClient.create());
    }

    public HttpBridge(String name, WebClient webClient) {
        super(name);
        this.webClient = webClient;
    }

    @Override
    public Mono<Void> transmit0(ContextAttrs attrs) {
        String uri = attrs.removeAttr(BridgeAttrNames.HTTP_URI);
        Map<String, Object> oHeaders = attrs.removeAttr(BridgeAttrNames.HTTP_HEADERS);

        return webClient
                .post()
                .uri(uri)
                .headers(headers -> oHeaders.forEach((k, v) -> headers.add(k, v.toString())))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(JSON.write(attrs))
                .retrieve()
                .toBodilessEntity()
                .then();
    }

    @Override
    public BridgeType type() {
        return BridgeType.HTTP;
    }
}
