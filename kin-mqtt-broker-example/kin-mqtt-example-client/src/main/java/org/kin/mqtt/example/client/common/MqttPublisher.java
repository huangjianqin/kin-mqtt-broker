package org.kin.mqtt.example.client.common;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/12/22
 */
public class MqttPublisher {
    public static void main(String[] args) {
        String broker = "tcp://127.0.0.1:1883";
        String topic = "MQTT Examples";

        String content = "Sample Message";
        //0 at most
        //1 at least
        //2 exactly
        int qos = 2;
        String clientId = "JavaPublisher";
        MemoryPersistence persistence = new MemoryPersistence();

        MqttClient sampleClient = null;
        try {
            sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName("java");
            connOpts.setPassword("12345".toCharArray());
            System.out.println("connecting to broker: " + broker);
            sampleClient.connect(connOpts);
            System.out.println("connected");
            System.out.println("publishing message: " + content);

            for (int i = 0; i < 1000; i++) {
                MqttMessage message = new MqttMessage((content + i).getBytes(StandardCharsets.UTF_8));
                message.setQos(qos);
                sampleClient.publish(topic, message);
                System.out.printf("message %d published\r\n", i);
                Thread.sleep(5_000);
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
}
