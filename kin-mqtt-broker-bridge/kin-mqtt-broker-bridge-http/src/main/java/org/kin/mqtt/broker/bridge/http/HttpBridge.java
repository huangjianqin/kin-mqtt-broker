package org.kin.mqtt.broker.bridge.http;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.bridge.NoErrorBridge;
import org.kin.mqtt.broker.rule.ContextAttrs;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;

import java.util.Map;

/**
 * 基于reactor-netty-http
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class HttpBridge extends NoErrorBridge {
    /** reactor http client */
    private final HttpClient httpClient;

    public HttpBridge() {
        this(BridgeType.HTTP.getDefaultName());
    }

    public HttpBridge(String name) {
        super(name);
        // TODO: 2023/3/30 设置http client
        this.httpClient = HttpClient.create();
    }

    @Override
    public Mono<Void> transmit0(ContextAttrs attrs) {
        String uri = attrs.removeAttr(BridgeAttrNames.HTTP_URI);
        Map<String, Object> oHeaders = attrs.removeAttr(BridgeAttrNames.HTTP_HEADERS);

        return httpClient
                .compress(true)
                .headers(headers -> {
                    oHeaders.forEach(headers::set);
                    headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
                })
                .post()
                .uri(uri)
                .send(ByteBufFlux.fromString(Mono.just(JSON.write(attrs))))
                .response()
                .then();
    }

    @Override
    public BridgeType type() {
        return BridgeType.HTTP;
    }
}
