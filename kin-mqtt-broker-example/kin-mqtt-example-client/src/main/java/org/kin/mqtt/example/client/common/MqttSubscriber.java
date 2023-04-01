package org.kin.mqtt.example.client.common;

import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttSubscription;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2022/12/22
 */
public class MqttSubscriber {
    private final String clientId;
    private final MemoryPersistence persistence = new MemoryPersistence();

    public MqttSubscriber(String clientId) {
        this.clientId = clientId;
    }

    /**
     * client subscribe
     */
    public void subscribe(String broker, String topic, CountDownLatch latch) throws InterruptedException {
        System.out.println("clientId: " + clientId);
        MqttClient client = null;
        try {
            client = new MqttClient(broker, clientId, persistence);
            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(true);
            connOpts.setUserName("java");
            connOpts.setPassword("12345".getBytes(StandardCharsets.UTF_8));
            System.out.println("connecting to broker: " + broker);
            client.connect(connOpts);
            System.out.println(broker + " connected ");
            System.out.println(broker + " start subscribe topic " + topic);
            //subscribe(String,int,IMqttMessageListener)会死循环
            client.subscribe(new MqttSubscription[]{new MqttSubscription(topic, 2)}, new IMqttMessageListener[]{
                    (s, mqttMessage) -> System.out.printf(System.currentTimeMillis() + ": receive from broker '%s': %d : %s : %s\r\n", broker, mqttMessage.getId(), s, new String(mqttMessage.getPayload(), StandardCharsets.UTF_8))});
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

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String topic = "example";

        MqttSubscriber subscriber = new MqttSubscriber("Subscriber");

//        ForkJoinPool.commonPool().execute(() -> {
//            try {
//                subscriber.subscribe("tcp://127.0.0.1:1883", topic, latch);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        });

        ForkJoinPool.commonPool().execute(() -> {
            try {
                subscriber.subscribe("tcp://127.0.0.1:1883", topic, latch);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(30_000);
        latch.await();
        Thread.sleep(1_000);
    }
}
