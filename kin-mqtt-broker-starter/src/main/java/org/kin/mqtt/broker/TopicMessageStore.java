package org.kin.mqtt.broker;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * mqtt publish消息存储
 * @author huangjianqin
 * @date 2022/11/6
 */
final class TopicMessageStore {
    /** 单例 */
    static final TopicMessageStore INSTANCE = new TopicMessageStore();

    /** key -> topic name , value -> publish消息Sink */
    final Map<String, Sinks.Many<ByteBuf>> topics = new ConcurrentHashMap<>();

    private TopicMessageStore() {
    }

    /**
     * publish消息
     * @param topicName topic name
     * @param bytes 消息payload
     */
    void publish(String topicName, ByteBuf bytes) {
        tryInitTopicMessageSink(topicName);
        topics.get(topicName).tryEmitNext(bytes);
    }

    /**
     * client / upstream rsocket service 订阅topic时触发
     * @param topicName topic name
     * @return  rsocket broker返回
     */
    Flux<ByteBuf> onSubscription(String topicName) {
        tryInitTopicMessageSink(topicName);
        return topics.get(topicName).asFlux();
    }

    /**
     * 初始化topic消息缓存
     * @param topicName topic name
     */
    private void tryInitTopicMessageSink(String topicName) {
        if (!topics.containsKey(topicName)) {
            topics.computeIfAbsent(topicName, k -> Sinks.many().replay().all());
        }
    }
}
