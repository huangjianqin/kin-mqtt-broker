package org.kin.mqtt.broker.store;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.utils.TopicUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * 基于内存存储mqtt message
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public class DefaultMqttMessageStore extends AbstractMqttMessageStore {
    private static final Logger log = LoggerFactory.getLogger(DefaultMqttMessageStore.class);

    /** key -> client id, value -> 离线接收的mqtt消息 */
    private final Map<String, List<MqttMessageReplica>> offlineMessages = new NonBlockingHashMap<>();
    /** key -> topic, value -> 该topic的retain消息 */
    private final Map<String, MqttMessageReplica> retainMessages = new NonBlockingHashMap<>();

    @Override
    public void saveOfflineMessage(String clientId,
                                   MqttMessageReplica replica) {
        List<MqttMessageReplica> replicaCache = offlineMessages.computeIfAbsent(clientId, k -> new CopyOnWriteArrayList<>());
        replicaCache.add(replica);
    }

    @Override
    public void saveOfflineMessages(String clientId, Collection<MqttMessageReplica> replicas) {
        if (CollectionUtils.isEmpty(replicas)) {
            return;
        }

        List<MqttMessageReplica> replicaCache = offlineMessages.computeIfAbsent(clientId, k -> new CopyOnWriteArrayList<>());
        replicaCache.addAll(replicas);
    }

    @Override
    public Flux<MqttMessageReplica> getAndRemoveOfflineMessage(String clientId) {
        List<MqttMessageReplica> replicas = offlineMessages.remove(clientId);
        if (Objects.isNull(replicas)) {
            replicas = Collections.emptyList();
        }
        return Flux.fromIterable(replicas);
    }

    @Override
    public void saveRetainMessage(MqttMessageReplica replica) {
        retainMessages.put(replica.getTopic(), replica);
    }

    @Override
    public Flux<MqttMessageReplica> getRetainMessage(String topic) {
        return Flux.fromIterable(retainMessages.entrySet()
                .stream()
                .filter(e -> e.getKey().matches(TopicUtils.toRegexTopic(topic)))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList()));
    }
}
