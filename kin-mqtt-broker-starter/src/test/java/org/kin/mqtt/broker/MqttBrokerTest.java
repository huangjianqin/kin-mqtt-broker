package org.kin.mqtt.broker;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2022/11/7
 */
@SpringBootApplication
public class MqttBrokerTest {
    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(MqttBrokerTest.class, args);

        //mqtt client
        CountDownLatch latch = new CountDownLatch(2);
        String broker = "tcp://127.0.0.1:1883";
        String topic = "MQTT Examples";
        ForkJoinPool.commonPool().execute(() -> {
            try {
                subscribe(broker, topic, latch);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        ForkJoinPool.commonPool().execute(() -> {
            try {
                publish(broker, topic, latch);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        latch.await();
        context.close();
    }

    /**
     * client subscribe
     */
    private static void subscribe(String broker, String topic, CountDownLatch latch) throws InterruptedException {
        String clientId = "JavaSubscriber";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient client = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("connecting to broker: " + broker);
            client.connect(connOpts);
            System.out.println("connected");
            System.out.println("start subscribe");
            client.subscribe(topic, (s, mqttMessage) -> {
                System.out.printf("receive: %d %s %s\r\n", mqttMessage.getId(), s, new String(mqttMessage.getPayload(), StandardCharsets.UTF_8));
            });
            System.out.println("subscribe success");

            Thread.sleep(10_000);

            client.disconnect();
            System.out.println("disconnected");
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("exception " + me);
            me.printStackTrace();
        } finally {
            latch.countDown();
        }
    }

    /**
     * client publish
     */
    private static void publish(String broker, String topic, CountDownLatch latch) throws InterruptedException {
        String content = "Sample Message";
        int qos = MqttQoS.EXACTLY_ONCE.value();
        String clientId = "JavaPublisher";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("connecting to broker: " + broker);
            sampleClient.connect(connOpts);
            System.out.println("connected");
            System.out.println("publishing message: " + content);

            for (int i = 0; i < 8; i++) {
                MqttMessage message = new MqttMessage((content + i).getBytes(StandardCharsets.UTF_8));
                message.setQos(qos);
                sampleClient.publish(topic, message);
                System.out.printf("message %d published\r\n", i);
                Thread.sleep(1_000);
            }

            Thread.sleep(1_000);
            sampleClient.disconnect();
            System.out.println("disconnected");
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("exception " + me);
            me.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}
