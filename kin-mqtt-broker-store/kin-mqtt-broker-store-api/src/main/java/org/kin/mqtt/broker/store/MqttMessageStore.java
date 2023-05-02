package org.kin.mqtt.broker.store;

import org.kin.framework.Closeable;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import reactor.core.publisher.Flux;

/**
 * mqtt消息外部存储
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public interface MqttMessageStore extends Closeable {
    /**
     * 保存mqtt client下线后接收到的消息
     *
     * @param clientId 接收到的mqtt client id
     * @param replica  mqtt消息副本
     */
    void saveOfflineMessage(String clientId,
                            MqttMessageReplica replica);

    /**
     * 获取mqtt client下线后接收到的消息
     *
     * @param clientId mqtt client id
     * @return 下线后接收到的消息
     */
    Flux<MqttMessageReplica> getAndRemoveOfflineMessage(String clientId);

    /**
     * 保留mqtt retain消息
     *
     * @param replica mqtt消息副本
     */
    void saveRetainMessage(MqttMessageReplica replica);

    /**
     * 获取mqtt retain消息
     *
     * @param topic topic
     * @return mqtt retain消息
     */
    Flux<MqttMessageReplica> getRetainMessage(String topic);
}
