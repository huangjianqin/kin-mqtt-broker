package org.kin.mqtt.broker.bridge.http;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.bridge.NoErrorBridge;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.kin.transport.netty.TransportCustomizer;
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
        this(TransportCustomizer.DO_NOTHING);
    }

    public HttpBridge(TransportCustomizer customizer) {
        this(DEFAULT_NAME, TransportCustomizer.DO_NOTHING);
    }

    public HttpBridge(String name) {
        this(name, TransportCustomizer.DO_NOTHING);
    }

    public HttpBridge(String name, TransportCustomizer customizer) {
        super(name);
        this.httpClient = customizer.custom(HttpClient.create());
    }

    @Override
    public Mono<Void> transmit0(ContextAttrs attrs) {
        String uri = attrs.rmAttr(BridgeAttrNames.HTTP_URI);
        Map<String, Object> oHeaders = attrs.rmAttr(BridgeAttrNames.HTTP_HEADERS);

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
