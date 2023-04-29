package org.kin.mqtt.broker.cluster.gossip;

import org.kin.mqtt.broker.cluster.MqttBrokerNode;

import java.util.List;

/**
 * 基于gossip的mqtt broker集群节点实现
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public class GossipNode implements MqttBrokerNode {
    /** broker name */
    private String name;
    /** host */
    private String host;
    /** 端口 */
    private Integer port;
    /** 命名空间 */
    private String namespace;
    /** 该broker节点订阅管理 */
    private final RemoteTopicSubscriptionManager subscriptionManager = new RemoteTopicSubscriptionManager();

    /**
     * 该mqtt broker节点是否有订阅{@code topic}的mqtt client
     *
     * @param topic mqtt topic
     */
    public boolean hasSubscription(String topic) {
        return subscriptionManager.hasSubscription(topic);
    }

    /**
     * remote mqtt broker订阅变化时, 维护该节点的订阅信息
     *
     * @param subscriptions mqtt topic
     */
    public void onSubscriptionChanged(List<RemoteTopicSubscription> changedSubscriptions) {
        subscriptionManager.onSubscriptionChanged(changedSubscriptions);
    }


    //--------------------------------------------------------------------------------------------------------------
    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final GossipNode node = new GossipNode();

        public Builder name(String name) {
            node.name = name;
            return this;
        }

        public Builder host(String host) {
            node.host = host;
            return this;
        }

        public Builder port(Integer port) {
            node.port = port;
            return this;
        }

        public Builder namespace(String namespace) {
            node.namespace = namespace;
            return this;
        }

        public GossipNode build() {
            return node;
        }
    }

    //getter
    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public Integer getPort() {
        return port;
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public String toString() {
        return "GossipNode{" +
                "name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", namespace='" + namespace + '\'' +
                ", subscriptionManager=" + subscriptionManager +
                '}';
    }
}
