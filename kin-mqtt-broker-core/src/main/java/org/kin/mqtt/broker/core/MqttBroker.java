package org.kin.mqtt.broker.core;

import org.kin.framework.Closeable;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.util.LinkedList;
import java.util.List;

/**
 * @author huangjianqin
 * @date 2022/11/12
 */
public final class MqttBroker implements Closeable {
    /** mqtt broker context */
    private final MqttBrokerContext context;
    /** mqtt broker disposables */
    private final List<Disposable> disposables = new LinkedList<>();

    /**
     * @param context                  mqtt broker context
     * @param disposableServerMonoList mqtt server disposable mono
     * @param resCleaner               其他资源清理逻辑
     */
    public MqttBroker(MqttBrokerContext context, List<Mono<DisposableServer>> disposableServerMonoList, Runnable resCleaner) {
        this.context = context;
        for (Mono<DisposableServer> disposableServerMono : disposableServerMonoList) {
            disposableServerMono.doOnNext(d -> {
                synchronized (MqttBroker.this) {
                    disposables.add(d);
                    if (disposables.size() == disposableServerMonoList.size() - 1) {
                        //最后一个disposable, 将清理资源逻辑与其绑定
                        d.onDispose(resCleaner::run);
                    }
                }
            }).subscribe();
        }
    }

    @Override
    public synchronized void close() {
        for (Disposable disposable : disposables) {
            disposable.dispose();
        }
    }

    //getter
    public MqttBrokerContext getContext() {
        return context;
    }
}
