package org.kin.mqtt.broker.core.cluster.gossip;

import org.kin.mqtt.broker.core.cluster.ClusterConfig;

/**
 * gossip配置
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public final class GossipConfig extends ClusterConfig {
    /** gossip暴露端口 */
    private int port;
    /** gossip集群命名空间 需要一致才能通信 */
    private String namespace;
    /** gossip集群seed节点配置, ';'分割 */
    private String seeds;

    private GossipConfig() {
    }

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final GossipConfig gossipConfig = new GossipConfig();

        public Builder port(int port) {
            gossipConfig.port = port;
            return this;
        }

        public Builder namespace(String namespace) {
            gossipConfig.namespace = namespace;
            return this;
        }

        public Builder seeds(String seeds) {
            gossipConfig.seeds = seeds;
            return this;
        }

        public GossipConfig build() {
            return gossipConfig;
        }
    }

    //getter
    public int getPort() {
        return port;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSeeds() {
        return seeds;
    }
}
