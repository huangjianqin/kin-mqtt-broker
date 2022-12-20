package org.kin.mqtt.broker.core;

import io.micrometer.core.instrument.Metrics;
import org.jctools.maps.NonBlockingHashMap;
import org.kin.mqtt.broker.metrics.MetricsNames;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
public class DefaultMqttChannelManager implements MqttChannelManager {
    /** key -> client id, value -> mqtt channel */
    private final Map<String, MqttChannel> clientId2Channel = new NonBlockingHashMap<>();

    @Override
    public boolean register(String clientId, MqttChannel mqttChannel) {
        reportOnline();
        return Objects.isNull(clientId2Channel.put(clientId, mqttChannel));
    }

    /**
     * 上报该broker连接的mqtt client数量
     */
    private void reportOnline() {
        Metrics.gauge(MetricsNames.ONLINE_NUM, this, manager -> manager.clientId2Channel.size());
    }

    @Override
    public MqttChannel get(String clientId) {
        return clientId2Channel.get(clientId);
    }

    @Override
    public MqttChannel remove(String clientId) {
        reportOnline();
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
