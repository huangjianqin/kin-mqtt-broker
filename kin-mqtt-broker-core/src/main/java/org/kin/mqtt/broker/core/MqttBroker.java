package org.kin.mqtt.broker.core;

import org.kin.framework.Closeable;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/11/12
 */
public final class MqttBroker implements Closeable {
    /** mqtt broker context */
    private final MqttBrokerContext context;
    /** mqtt server disposable */
    private volatile DisposableServer mqttServerDisposable;

    public MqttBroker(MqttBrokerContext context, Mono<DisposableServer> disposableServerMono) {
        this.context = context;
        disposableServerMono.doOnNext(d -> mqttServerDisposable = d).subscribe();
    }

    @Override
    public void close() {
        if (Objects.nonNull(mqttServerDisposable)) {
            mqttServerDisposable.dispose();
        }
    }

    //getter
    public MqttBrokerContext getContext() {
        return context;
    }
}
