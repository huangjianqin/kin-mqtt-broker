package org.kin.mqtt.broker.core;

import org.jctools.maps.NonBlockingHashMap;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
public final class DefaultMqttChannelManager implements MqttChannelManager {
    /** key -> client id, value -> mqtt channel */
    private final Map<String, MqttChannel> clientId2Channel = new NonBlockingHashMap<>();

    @Override
    public void register(String clientId, MqttChannel mqttChannel) {
        clientId2Channel.put(clientId, mqttChannel);
    }

    @Override
    public MqttChannel get(String clientId) {
        return clientId2Channel.get(clientId);
    }

    @Override
    public MqttChannel remove(String clientId) {
        return clientId2Channel.remove(clientId);
    }

    @Override
    public boolean contains(String clientId) {
        MqttChannel mqttChannel = clientId2Channel.get(clientId);
        return Objects.nonNull(mqttChannel) & mqttChannel.isOnline();
    }

    @Override
    public int size() {
        return clientId2Channel.size();
    }

    @Override
    public Collection<MqttChannel> all() {
        return clientId2Channel.values();
    }
}
