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
public class DefaultMqttSessionManager implements MqttSessionManager {
    /** key -> client id, value -> mqtt session */
    private final Map<String, MqttSession> clientId2Session = new NonBlockingHashMap<>();

    @Override
    public boolean register(String clientId, MqttSession mqttSession) {
        reportOnline();
        return Objects.isNull(clientId2Session.put(clientId, mqttSession));
    }

    /**
     * 上报该broker连接的mqtt client数量
     */
    private void reportOnline() {
        Metrics.gauge(MetricsNames.ONLINE_NUM, this, manager -> manager.clientId2Session.size());
    }

    @Override
    public MqttSession get(String clientId) {
        return clientId2Session.get(clientId);
    }

    @Override
    public MqttSession remove(String clientId) {
        reportOnline();
        return clientId2Session.remove(clientId);
    }

    @Override
    public boolean contains(String clientId) {
        MqttSession mqttSession = clientId2Session.get(clientId);
        return Objects.nonNull(mqttSession) & mqttSession.isOnline();
    }

    @Override
    public int size() {
        return clientId2Session.size();
    }

    @Override
    public Collection<MqttSession> all() {
        return clientId2Session.values();
    }
}
