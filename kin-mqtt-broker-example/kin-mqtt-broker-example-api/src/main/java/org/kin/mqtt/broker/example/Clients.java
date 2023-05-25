package org.kin.mqtt.broker.example;

/**
 * @author huangjianqin
 * @date 2023/5/25
 */
public interface Clients {
    String PUBLISHER = "publisher";
    String DELAY_PUBLISHER = "delayPublisher";
    String SHARE_TOPIC_PUBLISHER = "shareTopicPublisher";

    String SUBSCRIBER = "subscriber";
    String DELAY_SUBSCRIBER = "delaySubscriber";
    String AUTO_RECONNECT_SUBSCRIBER = "autoReconnectSubscriber";
    String SHARE_SESSION_SUBSCRIBER = "shareSessionSubscriber";
    String CONNECT_PER_SEC_TEST = "connectPerSecTest";
    String SHARE_TOPIC_SUBSCRIBER = "shareTopicSubscriber";
}
