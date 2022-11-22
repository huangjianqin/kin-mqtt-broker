package org.kin.mqtt.broker.bridge;

import org.kin.framework.log.LoggerOprs;
import org.kin.mqtt.broker.rule.ContextAttrs;
import reactor.core.publisher.Mono;

/**
 * 屏蔽异常但会打印异常的{@link Bridge}实现
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public abstract class NoErrorBridge extends NamedBridge implements LoggerOprs {
    protected NoErrorBridge() {
    }

    protected NoErrorBridge(String name) {
        super(name);
    }

    @Override
    public Mono<Void> transmit(ContextAttrs attrs) {
        try {
            return transmit0(attrs)
                    .doOnError(t -> error("", t))
                    .onErrorResume(t -> Mono.empty());
        } catch (Exception e) {
            error("", e);
            return Mono.empty();
        }
    }

    /**
     * 传输数据
     *
     * @param attrs 规则处理过程产生的数据
     * @return 传输数据complete signal
     */
    protected abstract Mono<Void> transmit0(ContextAttrs attrs);
}
