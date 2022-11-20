package org.kin.mqtt.broker.core.message;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Map;

/**
 * mqtt消息副本, 用于集群广播, 持久化等
 * todo 目前仅仅处理publish消息
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public final class MqttMessageReplica implements Serializable {
    private static final long serialVersionUID = 8598710380202474132L;

    /** 发送该mqtt消息的mqtt client */
    private String clientId;
    /** mqtt消息topic */
    private String topic;
    /** mqtt消息qos */
    private int qos;
    /** mqtt消息retain标识 */
    private boolean retain;
    /** mqtt消息payload */
    private byte[] message;
    /** 接收mqtt消息时间戳 */
    private long timestamp;
    /** mqtt消息可变头属性 */
    private Map<String, String> properties;

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final MqttMessageReplica mqttMessageReplica = new MqttMessageReplica();

        public Builder clientId(String clientId) {
            mqttMessageReplica.clientId = clientId;
            return this;
        }

        public Builder topic(String topic) {
            mqttMessageReplica.topic = topic;
            return this;
        }

        public Builder qos(int qos) {
            mqttMessageReplica.qos = qos;
            return this;
        }

        public Builder setRetain(boolean retain) {
            mqttMessageReplica.retain = retain;
            return this;
        }

        public Builder message(byte[] message) {
            mqttMessageReplica.message = message;
            return this;
        }

        public Builder timestamp(long timestamp) {
            mqttMessageReplica.timestamp = timestamp;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            mqttMessageReplica.properties = properties;
            return this;
        }

        public MqttMessageReplica build() {
            return mqttMessageReplica;
        }
    }

    //setter && getter
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "MqttMessageReplica{" +
                "clientId='" + clientId + '\'' +
                ", topic='" + topic + '\'' +
                ", qos=" + qos +
                ", retain=" + retain +
                ", timestamp=" + timestamp +
                ", properties=" + properties +
                '}';
    }
}
