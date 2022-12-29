package org.kin.mqtt.broker.core.will;

import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * 遗愿
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public class Will {
    /** will retain */
    private boolean retain;
    /** will topic */
    private String topic;
    /** will topic qos */
    private MqttQoS qoS;
    /** will message bytes */
    private byte[] message;
    /** 延迟发布will消息, 单位是秒, 0表示无延迟 */
    private int delay;

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final Will will = new Will();

        public Builder setRetain(boolean retain) {
            will.retain = retain;
            return this;
        }

        public Builder topic(String topic) {
            will.topic = topic;
            return this;
        }

        public Builder qoS(MqttQoS qoS) {
            will.qoS = qoS;
            return this;
        }

        public Builder message(byte[] message) {
            will.message = message;
            return this;
        }

        public Builder delay(int delay) {
            will.delay = delay;
            return this;
        }

        public Will build() {
            return will;
        }
    }

    //setter && getter
    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public MqttQoS getQoS() {
        return qoS;
    }

    public void setQoS(MqttQoS qoS) {
        this.qoS = qoS;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    @Override
    public String toString() {
        return "Will{" +
                "retain=" + retain +
                ", topic='" + topic + '\'' +
                ", qoS=" + qoS +
                ", delay=" + delay +
                '}';
    }
}
