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
    /** mqtt server disposable */
    private volatile DisposableServer mqttServerDisposable;

    public MqttBroker(Mono<DisposableServer> disposableServerMono) {
        disposableServerMono.doOnNext(d -> mqttServerDisposable = d).subscribe();
    }

    @Override
    public void close() {
        if (Objects.nonNull(mqttServerDisposable)) {
            mqttServerDisposable.dispose();
        }
    }
}
