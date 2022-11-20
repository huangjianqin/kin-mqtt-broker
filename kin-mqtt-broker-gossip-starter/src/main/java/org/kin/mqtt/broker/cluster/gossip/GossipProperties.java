package org.kin.mqtt.broker.cluster.gossip;

import org.kin.mqtt.broker.core.Constants;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConfigurationProperties(Constants.PROPERTIES_PREFIX + ".gossip")
public class GossipProperties {
    /** gossip暴露端口 */
    private int port;
    /** gossip集群命名空间 需要一致才能通信, 默认MqttBroker */
    private String namespace = "MqttBroker";
    /** gossip集群seed节点配置, ';'分割 */
    private String seeds;

    //setter && getter
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
}
