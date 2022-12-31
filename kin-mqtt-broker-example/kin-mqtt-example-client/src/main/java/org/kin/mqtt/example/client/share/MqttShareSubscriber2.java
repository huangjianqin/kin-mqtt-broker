package org.kin.mqtt.example.client.share;

import org.kin.mqtt.example.client.common.MqttSubscriber;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2022/12/31
 */
public class MqttShareSubscriber2 {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        MqttSubscriber subscriber = new MqttSubscriber("ShareSubscriber2");
        String topic = "$share/g1/example";

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
