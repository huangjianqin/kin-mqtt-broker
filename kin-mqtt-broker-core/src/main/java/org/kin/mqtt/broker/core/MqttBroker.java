package org.kin.mqtt.broker.core;

import org.kin.framework.Closeable;
import reactor.netty.DisposableServer;

/**
 * @author huangjianqin
 * @date 2022/11/12
 */
public final class MqttBroker implements Closeable {
    /** mqtt server disposable */
    private final DisposableServer mqttServerDisposable;

    public MqttBroker(DisposableServer mqttServerDisposable) {
        this.mqttServerDisposable = mqttServerDisposable;
    }

    @Override
    public void close() {
        mqttServerDisposable.dispose();
    }
}
