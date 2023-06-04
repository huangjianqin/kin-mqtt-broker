package org.kin.mqtt.broker.bridge.http.boot;

import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.IgnoreErrorBridge;
import org.kin.mqtt.broker.bridge.definition.HttpBridgeDefinition;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;

/**
 * 基于{@link org.springframework.web.reactive.function.client.WebClient}
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class HttpBridge extends IgnoreErrorBridge {
    private final WebClient webClient;
    /** 默认headers */
    private Map<String, String> headers;

    public HttpBridge(String name) {
        this(name, Collections.emptyMap());
    }

    public HttpBridge(String name, Map<String, String> headers) {
        this(name, WebClient.create(), headers);
    }

    public HttpBridge(String name, WebClient webClient) {
        this(name, webClient, Collections.emptyMap());
    }

    public HttpBridge(String name, WebClient webClient, Map<String, String> headers) {
        super(name);
        this.webClient = webClient;
        this.headers = headers;
    }

    public HttpBridge(HttpBridgeDefinition definition){
        this(definition.getName(), definition.getHeaders());
    }

    @Override
    protected Mono<Void> transmit0(ContextAttrs attrs) {
        String uri = attrs.removeAttr(BridgeAttrNames.HTTP_URI);
        Map<String, Object> oHeaders = attrs.removeAttr(BridgeAttrNames.HTTP_HEADERS);

        return webClient
                .post()
                .uri(uri)
                .headers(headers -> this.headers.forEach((k, v) -> headers.add(k, v.toString())))
                .headers(headers -> oHeaders.forEach((k, v) -> headers.add(k, v.toString())))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(JSON.write(attrs))
                .retrieve()
                .toBodilessEntity()
                .then();
    }
}
