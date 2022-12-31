package org.kin.mqtt.example.client.common;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2022/12/22
 */
public class MqttSubscriber {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String topic = "MQTT Examples";

//        ForkJoinPool.commonPool().execute(() -> {
//            try {
//                subscribe("tcp://127.0.0.1:1883", topic, latch);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        });

        ForkJoinPool.commonPool().execute(() -> {
            try {
                subscribe("tcp://127.0.0.1:1884", topic, latch);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(30_000);
        latch.await();
        Thread.sleep(1_000);
    }

    /**
     * client subscribe
     */
    private static void subscribe(String broker, String topic, CountDownLatch latch) throws InterruptedException {
        String clientId = "JavaSubscriber";
        MemoryPersistence persistence = new MemoryPersistence();

        MqttClient client = null;
        try {
            client = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName("java");
            connOpts.setPassword("12345".toCharArray());
            System.out.println("connecting to broker: " + broker);
            client.connect(connOpts);
            System.out.println(broker + " connected ");
            System.out.println(broker + " start subscribe");
            client.subscribe(topic, (s, mqttMessage) -> {
                System.out.printf("receive from '%s': %d %s %s\r\n", broker, mqttMessage.getId(), s, new String(mqttMessage.getPayload(), StandardCharsets.UTF_8));
            });
            System.out.println(broker + " subscribe success");

            latch.await();

            client.disconnect();
            System.out.println(broker + " disconnected");
        } catch (MqttException me) {
            System.out.println(broker + " reason " + me.getReasonCode());
            System.out.println(broker + " msg " + me.getMessage());
            System.out.println(broker + " loc " + me.getLocalizedMessage());
            System.out.println(broker + " cause " + me.getCause());
            System.out.println(broker + " exception " + me);
            me.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (Objects.nonNull(client)) {
                try {
                    client.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
