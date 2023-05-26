package org.kin.mqtt.broker.bridge.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.NoErrorBridge;
import org.kin.mqtt.broker.bridge.definition.KafkaBridgeDefinition;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.kin.mqtt.broker.rule.RuleCtxAttrNames;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于reactor-kafka
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class KafkaBridge extends NoErrorBridge {
    /** 默认kafka broker地址 */
    private final static String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    /**
     * 获取默认的kafka broker配置
     */
    private static Map<String, Object> getDefaultProps(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    private final KafkaSender<String, String> sender;

    public KafkaBridge(String name) {
        this(name, DEFAULT_BOOTSTRAP_SERVERS);
    }

    public KafkaBridge(String name, String bootstrapServers) {
        this(name, getDefaultProps(bootstrapServers));
    }

    public KafkaBridge(String name, Map<String, Object> props) {
        super(name);
        //overwrite
        props.put(ProducerConfig.CLIENT_ID_CONFIG, name + "-kafka-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        sender = KafkaSender.create(SenderOptions.create(props));
    }

    public KafkaBridge(KafkaBridgeDefinition definition) {
        this(definition.getName(), definition.getProps());
    }

    @Override
    protected Mono<Void> transmit0(ContextAttrs attrs) {
        return Mono.just(attrs)
                .map(ca -> {
                    String kafkaTopic = ca.removeAttr(BridgeAttrNames.KAFKA_TOPIC);
                    //mqtt client id
                    String clientId = ca.getAttr(RuleCtxAttrNames.MQTT_CLIENT_ID);

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
    public void close() {
        sender.close();
    }
}
