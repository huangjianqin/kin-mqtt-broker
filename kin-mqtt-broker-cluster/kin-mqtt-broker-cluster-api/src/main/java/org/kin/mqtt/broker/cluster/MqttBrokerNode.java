package org.kin.mqtt.broker.cluster;

/**
 * mqtt broker集群节点
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public final class MqttBrokerNode {
    /** broker name */
    private String name;
    /** host */
    private String host;
    /** 端口 */
    private Integer port;
    /** 命名空间 */
    private String namespace;

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final MqttBrokerNode mqttBrokerNode = new MqttBrokerNode();

        public Builder name(String name) {
            mqttBrokerNode.name = name;
            return this;
        }

        public Builder host(String host) {
            mqttBrokerNode.host = host;
            return this;
        }

        public Builder port(Integer port) {
            mqttBrokerNode.port = port;
            return this;
        }

        public Builder namespace(String namespace) {
            mqttBrokerNode.namespace = namespace;
            return this;
        }

        public MqttBrokerNode build() {
            return mqttBrokerNode;
        }
    }

    //setter && getter
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
