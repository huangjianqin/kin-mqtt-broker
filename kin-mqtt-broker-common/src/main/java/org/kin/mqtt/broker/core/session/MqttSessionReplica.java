package org.kin.mqtt.broker.core.session;

import org.kin.mqtt.broker.core.topic.TopicSubscriptionReplica;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 * mqtt session持久化数据
 *
 * @author huangjianqin
 * @date 2023/4/18
 */
public class MqttSessionReplica implements Serializable {
    private static final long serialVersionUID = 5299751708971113273L;

    /** 当前session所在的broker id */
    private String brokerId;
    /** mqtt client id */
    private String clientId;
    /**
     * mqtt client 首次connect时间
     * 持久化session在后续connect中并不会更新这个值, 该值表示该session第一次connect成功的时间
     */
    private long firstConnectTime;
    /** mqtt client connect时间, 校验通过后的时间 */
    private long connectTime;
    /** mqtt client 离线时间 */
    private long disConnectTime;
    /** 会话过期时间(秒), 0xFFFFFFFF即为永不过期 */
    private long expireTime;
    /** mqtt client 订阅 */
    private Set<TopicSubscriptionReplica> subscriptions;

    /**
     * 判断session是否过期
     *
     * @return session是否过期
     */
    public boolean isExpire() {
        return expireTime > 0 && System.currentTimeMillis() >= expireTime;
    }

    /**
     * 判断session是否过期
     *
     * @return session是否过期
     */
    public boolean isValid() {
        return !isExpire();
    }

    /**
     * 获取session过期间隔
     *
     * @return session过期间隔
     */
    public long getExpiryInternal() {
        return expireTime - System.currentTimeMillis();
    }

    /**
     * 该matt client session是否已离线
     */
    public boolean isDisConnect() {
        return disConnectTime > 0;
    }

    /**
     * 该matt client session是否已在线
     */
    public boolean isConnected() {
        return !isDisConnect();
    }

    //setter && getter
    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public long getFirstConnectTime() {
        return firstConnectTime;
    }

    public void setFirstConnectTime(long firstConnectTime) {
        this.firstConnectTime = firstConnectTime;
    }

    public long getConnectTime() {
        return connectTime;
    }

    public void setConnectTime(long connectTime) {
        this.connectTime = connectTime;
    }

    public long getDisConnectTime() {
        return disConnectTime;
    }

    public void setDisConnectTime(long disConnectTime) {
        this.disConnectTime = disConnectTime;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public Set<TopicSubscriptionReplica> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(Set<TopicSubscriptionReplica> subscriptions) {
        this.subscriptions = subscriptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MqttSessionReplica)) return false;
        MqttSessionReplica that = (MqttSessionReplica) o;
        return Objects.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId);
    }
}
