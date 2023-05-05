package org.kin.mqtt.broker.domain;

import com.google.common.base.Preconditions;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.kin.mqtt.broker.core.message.MqttMessageContext;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 待发送的qos>0 mqtt message
 * <p>
 * MQTT v5.0协议为CONNECT报文新增了一个Receive Maximum的属性, 达到限流效果
 * 官方对它的解释是:
 * 客户端告诉服务端客户端能同时处理的QoS为1和QoS为2的发布消息最大数量.
 * 没有机制可以限制服务端试图发送的QoS为0的发布消息.
 * 也就是说, 服务端可以在等待确认时使用不同的报文标识符向客户端发送后续的PUBLISH报文, 直到未被确认的报文数量到达Receive Maximum限制
 * <p>
 * inflight message一直缓存在内存中, 直至client close, 才会尝试存储到offline message queue, 待client重新上线后重试inflight message
 * 期间如果broker down, 则inflight message就会丢失
 *
 * @author huangjianqin
 * @date 2023/1/2
 */
public class InflightWindow {
    /** 队列最大大小 */
    private static final int MAX_QUEUE_SIZE = 128;
    /** 接收端愿意同时处理的QoS为1和2的PUBLISH消息最大数量 */
    private volatile int receiveMaximum;


    /** 待发送的qos>0消息缓存, 按先入先出的顺序存储 */
    private final Deque<MqttMessageContext<MqttPublishMessage>> queue = new LinkedList<>();
    /** 可发送消息配额 */
    private volatile int quota;

    public InflightWindow() {
        this(0);
    }

    public InflightWindow(int receiveMaximum) {
        Preconditions.checkArgument(receiveMaximum <= MAX_QUEUE_SIZE, "receiveMaximum must be less and equal than " + MAX_QUEUE_SIZE);

        this.receiveMaximum = receiveMaximum;
        this.quota = receiveMaximum;
    }

    /**
     * 取配额, 如果拿到了, 则可以发送mqtt消息
     *
     * @param messageContext mqtt publish消息上下文
     * @return 是否拿到配额
     */
    public boolean takeQuota(MqttMessageContext<MqttPublishMessage> messageContext) {
        if (receiveMaximum <= 0) {
            return true;
        }

        MqttPublishMessage message = messageContext.getMessage();
        if (message.fixedHeader().qosLevel().value() < 1) {
            //不考虑at most once
            return true;
        }

        synchronized (this) {
            if (quota > 0 && queue.size() < 1) {
                //有配额并且队列无等待
                quota--;
                return true;
            } else {
                queue.add(messageContext);
                return false;
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
    public MqttPublishMessage returnQuota() {
        if (receiveMaximum <= 0) {
            return null;
        }

        synchronized (this) {
            MqttMessageContext<MqttPublishMessage> messageContext;
            while (queue.size() > 0) {
                messageContext = queue.poll();
                if (!messageContext.isExpire()) {
                    //没有过期
                    return messageContext.getMessage();
                }
            }

            //1. queue没有inflight message
            //2. queue中所有inflight message都过期
            quota++;
            return null;
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

    /**
     * 取所有inflight mqtt message
     * 目前用于mqtt client close时缓存至offline message, 待mqtt client重连后, 重试
     */
    public Collection<MqttMessageContext<MqttPublishMessage>> drainInflightMqttMessages() {
        if (receiveMaximum < 1) {
            return Collections.emptyList();
        }

        synchronized (this) {
            //过滤掉过期的
            return new ArrayList<>(queue).stream()
                    .filter(mc -> !mc.isExpire())
                    .collect(Collectors.toList());
        }
    }

    //getter
    public int getReceiveMaximum() {
        return receiveMaximum;
    }

    /**
     * 不保证准确性
     */
    public int getQuota() {
        return quota;
    }
}
