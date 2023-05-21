package org.kin.mqtt.broker.example.store.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.boot.EnableMqttBroker;
import org.kin.mqtt.broker.boot.MqttBrokerProperties;
import org.kin.mqtt.broker.core.MqttBroker;
import org.kin.mqtt.broker.core.MqttMessageSender;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
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
    public ApplicationRunner sendMqttMessageLoop(@Autowired MqttBroker mqttBroker,
                                                 @Autowired MqttMessageSender mqttMessageSender,
                                                 @Autowired MqttBrokerProperties properties) {
        return args -> ForkJoinPool.commonPool().execute(() -> {
            if (!properties.getBrokerId().equals("B1")) {
                return;
            }
            int messageId = 1;
            while (true) {
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                String s = "broker-" + mqttBroker.getBrokerId() + " loop:" + messageId;
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(bytes.length);
                byteBuf.writeBytes(bytes);

                MqttPublishMessage pubMessage = MqttMessageHelper.createPublish(false,
                        MqttQoS.AT_LEAST_ONCE,
                        messageId++,
                        Topics.BROKER_LOOP,
                        byteBuf);
                mqttMessageSender.sendMessage("Subscriber", pubMessage).subscribe();
            }
        });
    }
}