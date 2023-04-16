package org.kin.mqtt.example.client.delay;

import org.kin.mqtt.broker.example.Topics;
import org.kin.mqtt.example.client.common.MqttSubscriber;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2022/12/22
 */
public class MqttDelaySubscriber {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        MqttSubscriber subscriber = new MqttSubscriber("DelaySubscriber");

        ForkJoinPool.commonPool().execute(() -> {
            try {
                subscriber.subscribe("tcp://127.0.0.1:1883", Topics.EXAMPLE, latch);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(30_000);
        latch.countDown();
        Thread.sleep(1_000);
    }
}
