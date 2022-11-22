package org.kin.mqtt.broker.bridge;

import org.kin.framework.Closeable;
import org.kin.mqtt.broker.rule.ContextAttrs;
import reactor.core.publisher.Mono;

/**
 * 消息数据桥接接口
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public interface Bridge extends Closeable {
    /** 默认Bridge name */
    String DEFAULT_NAME = "default";

    /**
     * 传输数据
     *
     * @param attrs 规则处理过程产生的数据
     * @return 传输数据complete signal
     */
    Mono<Void> transmit(ContextAttrs attrs);

    /**
     * @return 数据桥接类型
     */
    BridgeType type();

    /**
     * @return Bridge name, Bridge唯一标识
     */
    default String name() {
        return DEFAULT_NAME;
    }

    @Override
    default void close() {
        //默认do nothing
    }
}
