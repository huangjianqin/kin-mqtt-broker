package org.kin.mqtt.example.client.share;

import org.kin.mqtt.broker.example.Brokers;
import org.kin.mqtt.broker.example.Topics;
import org.kin.mqtt.example.client.common.MqttSubscriber;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2022/12/22
 */
public class MqttShareSubscriber1 {
    public static void main(String[] args) throws InterruptedException, IOException {
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

        System.in.read();
        System.out.println("disconnecting...");
        latch.countDown();
        Thread.sleep(5_000);
        System.out.println("exit.");
    }
}
