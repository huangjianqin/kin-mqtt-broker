package org.kin.mqtt.broker.cluster.gossip;

import org.kin.framework.utils.NetUtils;

import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
public class GossipConfig {
    /** 默认同步订阅信息间隔 */
    public static final long DEFAULT_SYNC_SUBSCRIPTION_MILLS = TimeUnit.MINUTES.toMillis(5);

    /** gossip暴露host */
    private String host = NetUtils.getIp();
    /** gossip暴露端口 */
    private int port;
    /** gossip集群命名空间 需要一致才能通信, 默认MqttBroker */
    private String namespace = "MqttBroker";
    /** gossip集群seed节点配置, ';'分割 */
    private String seeds;
    /** 同步订阅信息间隔, 默认5min */
    private long syncSubscriptionMills = DEFAULT_SYNC_SUBSCRIPTION_MILLS;

    //setter && getter

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getSeeds() {
        return seeds;
    }

    public void setSeeds(String seeds) {
        this.seeds = seeds;
    }

    public long getSyncSubscriptionMills() {
        return syncSubscriptionMills;
    }

    public void setSyncSubscriptionMills(long syncSubscriptionMills) {
        this.syncSubscriptionMills = syncSubscriptionMills;
    }
}
