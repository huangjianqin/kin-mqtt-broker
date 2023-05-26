package org.kin.mqtt.broker.bridge;

import org.kin.mqtt.broker.bridge.definition.BridgeDefinition;
import org.kin.mqtt.broker.rule.ContextAttrs;
import reactor.core.publisher.Mono;

/**
 * 桥接上下文
 * @author huangjianqin
 * @date 2023/5/26
 */
public class BridgeContext implements Bridge{
    private final BridgeDefinition definition;
    private final Bridge delegate;

    public BridgeContext(BridgeDefinition definition, Bridge delegate) {
        this.definition = definition;
        this.delegate = delegate;
    }

    @Override
    public Mono<Void> transmit(ContextAttrs attrs) {
        return delegate.transmit(attrs);
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void close() {
        delegate.close();
    }

    //getter
    public BridgeDefinition getDefinition() {
        return definition;
    }

    public Bridge getDelegate() {
        return delegate;
    }
}
