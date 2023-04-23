package org.kin.mqtt.example.client.share;

import org.kin.mqtt.broker.example.Brokers;
import org.kin.mqtt.broker.example.Topics;
import org.kin.mqtt.example.client.common.MqttSubscriber;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2022/12/22
 */
public class MqttShareSubscriber1 {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        MqttSubscriber subscriber = new MqttSubscriber("ShareSubscriber1");
        String topic = "$share/g1/" + Topics.EXAMPLE;

        ForkJoinPool.commonPool().execute(() -> {
            try {
                subscriber.subscribe(Brokers.B1, topic, latch);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(30_000);
        latch.countDown();
        Thread.sleep(1_000);
    }
}
