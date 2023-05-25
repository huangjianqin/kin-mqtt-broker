package org.kin.mqtt.example.client.common;

import org.kin.mqtt.broker.example.Brokers;
import org.kin.mqtt.broker.example.Clients;
import org.kin.mqtt.broker.example.Topics;

/**
 * @author huangjianqin
 * @date 2023/5/24
 */
public class MqttPublisher4 {
    public static void main(String[] args) {
        MqttPublisher publisher = new MqttPublisher(Clients.PUBLISHER);
        //0 at most
        //1 at least
        //2 exactly
        publisher.publish(Brokers.B4, Topics.EXAMPLE, "Sample Message for B4-", 2);
    }
}
