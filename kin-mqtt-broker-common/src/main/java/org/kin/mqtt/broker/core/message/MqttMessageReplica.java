package org.kin.mqtt.broker.core.message;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * mqtt消息副本, 用于集群广播, 持久化等
 * 目前仅仅处理publish消息
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public class MqttMessageReplica implements Serializable {
    private static final long serialVersionUID = 8598710380202474132L;

    /** broker标识 */
    private String brokerId;
    /** 发送该mqtt消息的mqtt client */
    private String clientId;
    /** mqtt消息topic */
    private String topic;
    /** mqtt消息qos */
    private int qos;
    /** mqtt消息retain标识 */
    private boolean retain;
    /** mqtt消息payload */
    private byte[] payload;
    /** 接收mqtt消息时间戳(毫秒) */
    private long recTime;
    /** 过期时间(毫秒) */
    private long expireTime;
    /** mqtt消息可变头属性 */
    private Map<String, String> properties = Collections.emptyMap();

    private MqttMessageReplica() {
    }

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final MqttMessageReplica mqttMessageReplica = new MqttMessageReplica();

        public Builder brokerId(String brokerId) {
            mqttMessageReplica.brokerId = brokerId;
            return this;
        }

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

        public Builder payload(byte[] payload) {
            mqttMessageReplica.payload = payload;
            return this;
        }

        public Builder recTime(long recTime) {
            mqttMessageReplica.recTime = recTime;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            mqttMessageReplica.properties = properties;
            return this;
        }

        public Builder expireTime(long expireTime) {
            mqttMessageReplica.expireTime = expireTime;
            return this;
        }

        public MqttMessageReplica build() {
            return mqttMessageReplica;
        }
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

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public long getRecTime() {
        return recTime;
    }

    public void setRecTime(long recTime) {
        this.recTime = recTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    @Override
    public String toString() {
        return "MqttMessageReplica{" +
                "brokerId='" + brokerId + '\'' +
                "clientId='" + clientId + '\'' +
                ", topic='" + topic + '\'' +
                ", qos=" + qos +
                ", retain=" + retain +
                ", payload=" + Arrays.toString(payload) +
                ", timestamp=" + recTime +
                ", expireTime=" + expireTime +
                ", properties=" + properties +
                '}';
    }
}
