package org.kin.mqtt.broker.example.store.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.internal.ThreadLocalRandom;
import org.kin.mqtt.broker.boot.EnableMqttBroker;
import org.kin.mqtt.broker.core.MqttBroker;
import org.kin.mqtt.broker.core.MqttMessageSender;
import org.kin.mqtt.broker.example.Clients;
import org.kin.mqtt.broker.example.Topics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2023/4/22
 */
@EnableMqttBroker
@SpringBootApplication
public class MqttBrokerRedisStoreApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqttBrokerRedisStoreApplication.class, args);
    }

    @Bean
    public ApplicationRunner sendMqttMessageLoop1(@Autowired MqttBroker mqttBroker,
                                                  @Autowired MqttMessageSender mqttMessageSender) {
        return sendMqttMessageLoop(Clients.SUBSCRIBER, mqttBroker, mqttMessageSender);
    }

    @Bean
    public ApplicationRunner sendMqttMessageLoop2(@Autowired MqttBroker mqttBroker,
                                                  @Autowired MqttMessageSender mqttMessageSender) {
        return sendMqttMessageLoop(Clients.SHARE_SESSION_SUBSCRIBER, mqttBroker, mqttMessageSender);
    }


    private ApplicationRunner sendMqttMessageLoop(String targetClientId, MqttBroker mqttBroker, MqttMessageSender mqttMessageSender) {
        return args -> ForkJoinPool.commonPool().execute(() -> {
            int messageId = 1;
            while (true) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(4_500) + 500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                String s = "broker-" + mqttBroker.getBrokerId() + " loop:" + messageId++;
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                ByteBuf byteBuf = Unpooled.buffer(bytes.length);
                byteBuf.writeBytes(bytes);

                mqttMessageSender.sendMessage(targetClientId, Topics.BROKER_LOOP, MqttQoS.AT_LEAST_ONCE, byteBuf)
                        .subscribe();
            }
        });
    }
}