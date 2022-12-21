package org.kin.mqtt.broker.core;

import java.util.Collection;

/**
 * mqtt channel管理
 * todo 持久化session支持存库和集群共享
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public interface MqttChannelManager {
    /**
     * 注册新连接的mqtt client
     *
     * @param clientId    mqtt client id
     * @param mqttChannel mqtt channel
     * @return 是否首次注册, 对于持久化session的mqtt client, 则只有第一次注册才返回true
     */
    boolean register(String clientId, MqttChannel mqttChannel);

    /**
     * 根据client id获取mqtt channel
     *
     * @param clientId mqtt client id
     * @return mqtt channel
     */
    MqttChannel get(String clientId);

    /**
     * 移除指定client id的mqtt channel
     *
     * @param clientId mqtt client id
     * @return 是否移除成功
     */
    MqttChannel remove(String clientId);

    /**
     * 是否包含指定client id的mqtt channel
     *
     * @param clientId mqtt client id
     * @return 是否包含指定client id的mqtt channel
     */
    boolean contains(String clientId);

    /**
     * 已注册的mqtt client数
     *
     * @return 已注册的mqtt client数
     */
    int size();

    /**
     * 获取所有已注册mqtt channel
     *
     * @return 所有已注册mqtt channel
     */
    Collection<MqttChannel> all();
}
