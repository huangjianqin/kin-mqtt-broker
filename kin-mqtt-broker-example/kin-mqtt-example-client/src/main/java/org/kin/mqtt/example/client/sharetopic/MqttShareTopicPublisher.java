package org.kin.mqtt.example.client.sharetopic;

import org.kin.mqtt.broker.example.Brokers;
import org.kin.mqtt.broker.example.Clients;
import org.kin.mqtt.broker.example.Topics;
import org.kin.mqtt.example.client.common.MqttPublisher;

/**
 * @author huangjianqin
 * @date 2022/12/22
 */
public class MqttShareTopicPublisher {
    public static void main(String[] args) {
        MqttPublisher publisher = new MqttPublisher(Clients.SHARE_TOPIC_PUBLISHER);
        //0 at most
        //1 at least
        //2 exactly
        publisher.publish(Brokers.B1, Topics.EXAMPLE, "Sample Message", 2);
    }
}
