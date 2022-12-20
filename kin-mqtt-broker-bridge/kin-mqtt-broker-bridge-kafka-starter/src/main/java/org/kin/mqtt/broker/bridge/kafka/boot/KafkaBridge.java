package org.kin.mqtt.broker.bridge.kafka.boot;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.bridge.NoErrorBridge;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.kin.mqtt.broker.rule.RuleChainAttrNames;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;

/**
 * 基于{@link ReactiveKafkaProducerTemplate}
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class KafkaBridge extends NoErrorBridge {
    private final ReactiveKafkaProducerTemplate<String, String> sender;

    public KafkaBridge(ReactiveKafkaProducerTemplate<String, String> sender) {
        this.sender = sender;
    }

    public KafkaBridge(String name, ReactiveKafkaProducerTemplate<String, String> sender) {
        super(name);
        this.sender = sender;
    }

    @Override
    protected Mono<Void> transmit0(ContextAttrs attrs) {
        return Mono.just(attrs)
                .map(ca -> {
                    String kafkaTopic = ca.rmAttr(BridgeAttrNames.KAFKA_TOPIC);
                    //mqtt client id
                    String clientId = ca.getAttr(RuleChainAttrNames.MQTT_CLIENT_ID);

                    return SenderRecord.create(new ProducerRecord<>(kafkaTopic, clientId, JSON.write(attrs)), attrs.toString());
                })
                .flatMapMany(r -> sender.send(Mono.just(r))
                        .doOnNext(result -> {
                            RecordMetadata metadata = result.recordMetadata();
                            debug("kafka message '{}' sent successfully, topic-partition={}-{} offset={} timestamp={}",
                                    result.correlationMetadata(),
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset(),
                                    Instant.ofEpochMilli(metadata.timestamp()));
                        }))
                .then();
    }

    @Override
    public BridgeType type() {
        return BridgeType.KAFKA;
    }
}
