package org.kin.mqtt.broker.example;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.internal.ThreadLocalRandom;
import org.kin.mqtt.broker.boot.EnableMqttBroker;
import org.kin.mqtt.broker.core.MqttBroker;
import org.kin.mqtt.broker.core.MqttMessageSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ForkJoinPool;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@EnableMqttBroker
@SpringBootApplication
public class MqttBrokerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqttBrokerApplication.class, args);
    }

    @Bean
    public ApplicationRunner sendMqttMessageLoop(@Autowired MqttBroker mqttBroker,
                                                 @Autowired MqttMessageSender mqttMessageSender) {
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
                ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(bytes.length);
                byteBuf.writeBytes(bytes);

                mqttMessageSender.sendMessage("Subscriber", Topics.BROKER_LOOP, MqttQoS.AT_LEAST_ONCE, byteBuf)
                        .subscribe();
            }
        });
    }
}
