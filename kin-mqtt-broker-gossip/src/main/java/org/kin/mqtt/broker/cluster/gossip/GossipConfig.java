package org.kin.mqtt.broker.cluster.gossip;

import org.kin.framework.utils.NetUtils;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
public class GossipConfig {
    /** gossip暴露host */
    private String host = NetUtils.getIp();
    /** gossip暴露端口 */
    private int port;
    /** gossip节点别名 */
    private String alias;
    /** gossip集群命名空间 需要一致才能通信, 默认MqttBroker */
    private String namespace = "MqttBroker";
    /** gossip集群seed节点配置, ';'分割 */
    private String seeds;

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

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
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
}
