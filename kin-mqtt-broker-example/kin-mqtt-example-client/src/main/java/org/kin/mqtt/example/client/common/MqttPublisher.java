package org.kin.mqtt.example.client.common;


import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.kin.mqtt.broker.example.Brokers;
import org.kin.mqtt.broker.example.Topics;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/12/22
 */
public class MqttPublisher {
    private final String clientId;
    private final MemoryPersistence persistence = new MemoryPersistence();

    public MqttPublisher(String clientId) {
        this.clientId = clientId;
    }

    /**
     * mqtt client publish
     */
    public void publish(String broker, String topic, String content, int qos) {
        System.out.println("clientId: " + clientId);
        MqttClient sampleClient = null;
        try {
            sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(true);
            connOpts.setUserName("java");
            connOpts.setPassword("12345".getBytes(StandardCharsets.UTF_8));
            System.out.println("connecting to broker: " + broker);
            sampleClient.connect(connOpts);
            System.out.println("connected");
            System.out.printf("publishing message topic '%s': %s\r\n", topic, content);

            for (int i = 0; i < 1000; i++) {
                MqttMessage message = new MqttMessage((content + i).getBytes(StandardCharsets.UTF_8));
                message.setQos(qos);
                sampleClient.publish(topic, message);
                System.out.printf(System.currentTimeMillis() + ": message %d published\r\n", i);
                Thread.sleep(1_000);
            }

            sampleClient.disconnect();
            System.out.println("disconnected");
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("exception " + me);
            me.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (Objects.nonNull(sampleClient)) {
                try {
                    sampleClient.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        MqttPublisher publisher = new MqttPublisher("Publisher");
        //0 at most
        //1 at least
        //2 exactly
        publisher.publish(Brokers.B1, Topics.EXAMPLE, "Sample Message for p1-", 2);
    }
}
