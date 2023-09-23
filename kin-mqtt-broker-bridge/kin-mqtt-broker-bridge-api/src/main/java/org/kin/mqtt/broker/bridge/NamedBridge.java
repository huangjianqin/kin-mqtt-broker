package org.kin.mqtt.broker.bridge;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * @author huangjianqin
 * @date 2022/11/22
 */
public abstract class NamedBridge implements Bridge {
    private static final Logger log = LoggerFactory.getLogger(NamedBridge.class);

    /** bridge name */
    private final String name;

    public NamedBridge(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "bridge name must not blank");
        this.name = name;
    }

    @Override
    public final Mono<Void> transmit(ContextAttrs attrs) {
        try {
            return transmit0(attrs)
                    .doOnError(t -> log.error("", t))
                    .onErrorResume(t -> Mono.empty());
        } catch (Exception e) {
            log.error("", e);
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

    //getter
    @Override
    public String name() {
        return name;
    }
}
