package org.kin.mqtt.example.client.rate;

import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.kin.mqtt.broker.example.Brokers;
import org.kin.mqtt.broker.example.Clients;

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
        String broker = Brokers.B1;
        String clientId = Clients.CONNECT_PER_SEC_TEST;
        for (int i = 0; i < 20; i++) {
            int finalI = i;
            executors.execute(() -> connect(broker, clientId + finalI));
        }

        Thread.sleep(8_000);
        executors.shutdownNow();

        /*
        强行exit
        bug!!! eclipse mqtt client底层CommsReceiver, 即使强行disconnect或者close, 仍然会不断自旋
         */
        System.exit(0);
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
        } catch (Exception e) {
            System.out.println(clientId + " exception " + e);
            if (Objects.nonNull(sampleClient)) {
                try {
                    sampleClient.disconnectForcibly(1000, 1000, false);
                    System.out.println(String.format("%d %s disconnected forcibly", System.currentTimeMillis(), clientId));
                } catch (Exception e1) {
//                    System.err.println(e1);
                }
            }
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
