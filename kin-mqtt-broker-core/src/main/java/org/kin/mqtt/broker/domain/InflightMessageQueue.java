package org.kin.mqtt.broker.domain;

import io.netty.handler.codec.mqtt.MqttMessage;

import javax.annotation.Nullable;
import java.util.*;

/**
 * 待发送的qos>0 mqtt message
 *
 * @author huangjianqin
 * @date 2023/1/2
 */
public class InflightMessageQueue {
    /** 缓存最大大小 */
    public static final int MAX_QUEUE_SIZE = 128;

    /** 接收端愿意同时处理的QoS为1和2的PUBLISH消息最大数量 */
    private volatile int receiveMaximum;
    /** 待发送的qos>0消息缓存, 按先入先出的顺序存储 */
    private final Queue<MqttMessage> queue = new LinkedList<>();
    /** 可发送消息配额 */
    private int quota;

    public InflightMessageQueue() {
        this(0);
    }

    public InflightMessageQueue(int receiveMaximum) {
        this.receiveMaximum = receiveMaximum;
        this.quota = receiveMaximum;
    }

    /**
     * 取配额, 如果拿到了, 则可以发送mqtt消息
     *
     * @param message mqtt消息
     * @return inflight message queue大小
     */
    public int takeQuota(MqttMessage message) {
        if (receiveMaximum <= 0) {
            return 0;
        }

        if (message.fixedHeader().qosLevel().value() < 1) {
            //不考虑at most once
            return 0;
        }

        synchronized (this) {
            if (quota > 0) {
                quota--;
                return 0;
            } else {
                queue.add(message);
                return queue.size();
            }
        }
    }

    /**
     * 取inflight mqtt message, 如果有足够配额, 则扣除并返回
     * 目前用于mqtt client重连时, 重发mqtt message
     */
    public Collection<MqttMessage> takeInflightMqttMessages() {
        if (receiveMaximum > 0) {
            synchronized (this) {
                if (quota > 0) {
                    Collection<MqttMessage> messages = new ArrayList<>(quota);
                    MqttMessage message;
                    while (quota > 0 && Objects.nonNull((message = queue.poll()))) {
                        quota--;
                        messages.add(message);
                    }
                    return messages;
                } else {
                    return Collections.emptyList();
                }
            }
        } else {
            synchronized (this) {
                Collection<MqttMessage> messages = new ArrayList<>(queue.size());
                MqttMessage message;
                while (Objects.nonNull((message = queue.poll()))) {
                    messages.add(message);
                }

                return messages;
            }
        }
    }

    /**
     * 归还配额, 判断是否有inflight消息,
     * 如果有, 不归还quota, 直接取first, 然后send
     * 否则归还quota
     *
     * @return inflight消息
     */
    @Nullable
    public MqttMessage returnQuota() {
        if (receiveMaximum <= 0) {
            return null;
        }

        synchronized (this) {
            if (queue.size() > 0) {
                return queue.poll();
            } else {
                quota++;
                return null;
            }
        }
    }

    /**
     * 更新receive maximum
     */
    public void updateReceiveMaximum(int receiveMaximum) {
        if (this.receiveMaximum == receiveMaximum) {
            return;
        }

        synchronized (this) {
            int oldReceiveMaximum = this.receiveMaximum;
            this.receiveMaximum = receiveMaximum;
            //补差
            int diff = this.receiveMaximum - oldReceiveMaximum;
            quota += diff;
        }
    }

    //getter
    public int getReceiveMaximum() {
        return receiveMaximum;
    }
}
