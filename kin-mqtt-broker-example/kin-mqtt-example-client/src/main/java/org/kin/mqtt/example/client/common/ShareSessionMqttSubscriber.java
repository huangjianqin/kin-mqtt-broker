package org.kin.mqtt.example.client.common;

import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.kin.mqtt.broker.example.Brokers;
import org.kin.mqtt.broker.example.Topics;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * 跨broker节点共享session的subscriber
 *
 * @author huangjianqin
 * @date 2023/4/22
 */
public class ShareSessionMqttSubscriber {
    public static void main(String[] args) throws InterruptedException, IOException {
        CountDownLatch latch = new CountDownLatch(1);

        MqttSubscriber subscriber = new MqttSubscriber("Subscriber");

        ForkJoinPool.commonPool().execute(() -> {
            try {
                subscriber.subscribe(Brokers.B1, Topics.EXAMPLE, latch);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        System.in.read();
        System.out.println("disconnecting...");
        latch.countDown();
        Thread.sleep(5_000);
        System.out.println("exit.");
    }

    private static class MqttSubscriber {
        private final String clientId;
        private final MemoryPersistence persistence = new MemoryPersistence();

        public MqttSubscriber(String clientId) {
            this.clientId = clientId;
        }

        /**
         * client subscribe
         */
        public void subscribe(String broker, String topic, CountDownLatch latch) throws InterruptedException {
            MqttClient client = null;
            try {
                client = init(null, broker, topic);

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

        private MqttClient init(MqttClient client, String broker, String topic) {
            System.out.println("clientId: " + clientId);
            try {
                if (Objects.isNull(client)) {
                    client = new MqttClient(broker, clientId, persistence);
                }
                MqttConnectionOptions connOpts = new MqttConnectionOptions();
                connOpts.setCleanStart(false);
                //设置session有效期为1min
                connOpts.setSessionExpiryInterval(TimeUnit.MINUTES.toSeconds(1));
                connOpts.setUserName("java");
                connOpts.setPassword("12345".getBytes(StandardCharsets.UTF_8));
                //设置3个broker, 断连后重连
                connOpts.setAutomaticReconnect(true);
                connOpts.setServerURIs(Brokers.ALL);
                System.out.println("connecting to broker: " + broker);
                IMqttToken connAck = client.connectWithResult(connOpts);
                System.out.println(broker + " connected ");
                if (!connAck.getSessionPresent()) {
                    System.out.println(broker + " start subscribe topic " + topic);
                    IMqttMessageListener[] listeners = {
                            (s, mqttMessage) -> System.out.printf(System.currentTimeMillis() + ": receive from broker '%s': %d : %s : %s\r\n",
                                    broker, mqttMessage.getId(), s, new String(mqttMessage.getPayload(), StandardCharsets.UTF_8))
                    };
                    client.subscribe(new MqttSubscription[]{new MqttSubscription(topic, 2)}, listeners);
                    client.subscribe(new MqttSubscription[]{new MqttSubscription(Topics.BROKER_LOOP, 1)}, listeners);
                    System.out.println(broker + " subscribe success");
                }
            } catch (MqttException me) {
                System.out.println(broker + " reason " + me.getReasonCode());
                System.out.println(broker + " msg " + me.getMessage());
                System.out.println(broker + " loc " + me.getLocalizedMessage());
                System.out.println(broker + " cause " + me.getCause());
                System.out.println(broker + " exception " + me);
                me.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return client;
        }
    }
}
