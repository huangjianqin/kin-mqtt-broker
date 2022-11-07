package org.kin.mqtt.broker;

import io.netty.buffer.ByteBuf;

/**
 * publish mqtt消息服务
 * @author huangjianqin
 * @date 2022/11/6
 */
public final class PublishService {
    /** 单例 */
    public static final PublishService INSTANCE = new PublishService();

    private PublishService() {
    }

    /**
     * 指定topic publish消息
     */
    public void publish(String topic, ByteBuf payload) {
        TopicMessageStore.INSTANCE.publish(topic, payload);
    }
}
