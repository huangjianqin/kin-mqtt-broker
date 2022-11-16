package org.kin.mqtt.broker.core.store;

import org.kin.mqtt.broker.core.message.MqttMessageReplica;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * mqtt消息外部存储
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public interface MqttMessageStore {
    /**
     * 保存mqtt client下线后接收到的消息
     *
     * @param replica mqtt消息副本
     */
    void saveOfflineMessage(MqttMessageReplica replica);

    /**
     * 获取mqtt client下线后接收到的消息
     *
     * @param clientId mqtt client id
     * @return 下线后接收到的消息
     */
    @Nonnull
    List<MqttMessageReplica> getOfflineMessage(String clientId);

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
    @Nonnull
    List<MqttMessageReplica> getRetainMessage(String topic);
}
