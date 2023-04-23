package org.kin.mqtt.broker.core;

import java.util.Collection;

/**
 * 在线mqtt session管理
 * todo 设计优化
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public interface MqttSessionManager {
    /**
     * 注册新连接的mqtt client
     *
     * @param clientId    mqtt client id
     * @param mqttSession mqtt session
     * @return 是否首次注册, 对于持久化session的mqtt client, 则只有第一次注册才返回true
     */
    boolean register(String clientId, MqttSession mqttSession);

    /**
     * 根据client id获取mqtt session
     *
     * @param clientId mqtt client id
     * @return mqtt session
     */
    MqttSession get(String clientId);

    /**
     * 移除指定client id的mqtt session
     *
     * @param clientId mqtt client id
     * @return 是否移除成功
     */
    MqttSession remove(String clientId);

    /**
     * 是否包含指定client id的mqtt session
     *
     * @param clientId mqtt client id
     * @return 是否包含指定client id的mqtt session
     */
    boolean contains(String clientId);

    /**
     * 已注册的mqtt client数
     *
     * @return 已注册的mqtt client数
     */
    int size();

    /**
     * 获取所有已注册mqtt session
     *
     * @return 所有已注册mqtt session
     */
    Collection<MqttSession> all();
}
