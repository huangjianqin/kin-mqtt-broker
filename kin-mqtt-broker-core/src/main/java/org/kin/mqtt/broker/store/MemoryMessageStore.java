package org.kin.mqtt.broker.store;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * 基于内存存储
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public final class MemoryMessageStore extends AbstractMessageStore {
    /** key -> client id, value -> 离线接收的mqtt消息 */
    private final Map<String, List<MqttMessageReplica>> offlineMessages = new NonBlockingHashMap<>();
    /** key -> topic, value -> 该topic的retain消息 */
    private final Map<String, MqttMessageReplica> retainMessages = new NonBlockingHashMap<>();

    @Override
    public void saveOfflineMessage(MqttMessageReplica replica) {
        List<MqttMessageReplica> replicas = offlineMessages.computeIfAbsent(replica.getClientId(), k -> new CopyOnWriteArrayList<>());
        replicas.add(replica);
    }

    @Override
    public List<MqttMessageReplica> getOfflineMessage(String clientId) {
        List<MqttMessageReplica> replicas = offlineMessages.remove(clientId);
        if (Objects.isNull(replicas)) {
            replicas = Collections.emptyList();
        }
        return replicas;
    }

    @Override
    public void saveRetainMessage(MqttMessageReplica replica) {
        retainMessages.put(replica.getTopic(), replica);
    }

    @Override
    public List<MqttMessageReplica> getRetainMessage(String topic) {
        return retainMessages.entrySet()
                .stream()
                .filter(e -> e.getKey().matches(toRegexTopic(topic)))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }
}
