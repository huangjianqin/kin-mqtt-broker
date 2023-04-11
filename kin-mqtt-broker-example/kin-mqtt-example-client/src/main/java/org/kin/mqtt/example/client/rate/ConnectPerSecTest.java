package org.kin.mqtt.example.client.rate;

import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author huangjianqin
 * @date 2023/4/11
 */
public class ConnectPerSecTest {
    public static void main(String[] args) throws InterruptedException {
        ExecutorService executors = Executors.newCachedThreadPool();
        String broker = "tcp://127.0.0.1:1883";
        String clientId = "ConnectPerSecTest";
        for (int i = 0; i < 20; i++) {
            int finalI = i;
            executors.execute(() -> connect(broker, clientId + finalI));
        }

        Thread.sleep(8_000);
        executors.shutdownNow();
    }

    private static void connect(String broker, String clientId) {
        MqttClient sampleClient = null;
        try {
            sampleClient = new MqttClient(broker, clientId, new MemoryPersistence());
            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(true);
            connOpts.setUserName("java");
            connOpts.setPassword("12345".getBytes(StandardCharsets.UTF_8));
            System.out.println(String.format("%d %s connecting to broker: %s", System.currentTimeMillis(), clientId, broker));
            sampleClient.connect(connOpts);
            System.out.println(String.format("%d %s connected", System.currentTimeMillis(), clientId));

            Thread.sleep(5_000);

            sampleClient.disconnect();
            System.out.println(String.format("%d %s disconnected", System.currentTimeMillis(), clientId));
        } catch (MqttException me) {
//            System.out.println(clientId + " reason " + me.getReasonCode());
//            System.out.println(clientId + " msg " + me.getMessage());
//            System.out.println(clientId + " loc " + me.getLocalizedMessage());
//            System.out.println(clientId + " cause " + me.getCause());
//            System.out.println(clientId + " exception " + me);
//            System.err.println(me);
        } catch (Exception e) {
//            System.err.println(e);
        } finally {
            if (Objects.nonNull(sampleClient)) {
                try {
                    sampleClient.close(true);
                } catch (Exception e) {
//                    System.err.println(e);
                }
            }
        }
    }
}
