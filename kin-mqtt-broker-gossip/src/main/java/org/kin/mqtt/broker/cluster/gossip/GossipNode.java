package org.kin.mqtt.broker.cluster.gossip;

import org.jctools.maps.NonBlockingHashSet;
import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.cluster.MqttBrokerNode;

import java.util.Collection;
import java.util.Set;

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
    /** 该节点订阅topic的正则表达式 */
    private final Set<String> subscriptionTopicRegex = new NonBlockingHashSet<>();

    /**
     * 该mqtt broker节点是否有订阅{@code topic}的mqtt client
     *
     * @param topic mqtt topic
     */
    public boolean hasSubscription(String topic) {
        for (String topicRegex : subscriptionTopicRegex) {
            if (topic.matches(topicRegex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 新增订阅topic
     *
     * @param subscriptions mqtt topic
     */
    public void addSubscriptions(Collection<String> subscriptions) {
        subscriptionTopicRegex.addAll(subscriptions);
    }

    /**
     * 移除订阅topic
     *
     * @param subscriptions mqtt topic
     */
    public void removeSubscriptions(Collection<String> subscriptions) {
        if (CollectionUtils.isEmpty(subscriptions)) {
            return;
        }
        subscriptionTopicRegex.removeAll(subscriptions);
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
}
