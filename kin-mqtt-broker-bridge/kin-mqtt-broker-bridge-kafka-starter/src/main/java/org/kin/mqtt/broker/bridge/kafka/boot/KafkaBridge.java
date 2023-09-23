package org.kin.mqtt.broker.bridge.kafka.boot;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeConfiguration;
import org.kin.mqtt.broker.bridge.NamedBridge;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.kin.mqtt.broker.rule.RuleCtxAttrNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于{@link ReactiveKafkaProducerTemplate}
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class KafkaBridge extends NamedBridge {
    private static final Logger log = LoggerFactory.getLogger(KafkaBridge.class);
    /** 默认kafka broker地址 */
    private final static String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private final ReactiveKafkaProducerTemplate<String, String> sender;

    /**
     * 获取默认的kafka broker配置
     */
    private static Map<String, Object> getDefaultProps(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    /**
     * 获取默认的kafka broker配置
     */
    private static Map<String, Object> getDefaultProps(String name, Map<String, Object> props) {
        //overwrite
        props.put(ProducerConfig.CLIENT_ID_CONFIG, name + "-kafka-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    public KafkaBridge(String name) {
        this(name, DEFAULT_BOOTSTRAP_SERVERS);
    }

    public KafkaBridge(String name, String bootstrapServers) {
        this(name, getDefaultProps(bootstrapServers));
    }

    public KafkaBridge(String name, Map<String, Object> props) {
        this(name, new ReactiveKafkaProducerTemplate<>(SenderOptions.create(getDefaultProps(name, props))));
    }

    public KafkaBridge(String name, ReactiveKafkaProducerTemplate<String, String> sender) {
        super(name);
        this.sender = sender;
    }

    public KafkaBridge(BridgeConfiguration config) {
        this(config.getName(), config.toMap());
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
                            if (log.isDebugEnabled()) {
                                log.debug("kafka message '{}' sent successfully, topic-partition={}-{} offset={} timestamp={}",
                                        result.correlationMetadata(),
                                        metadata.topic(),
                                        metadata.partition(),
                                        metadata.offset(),
                                        Instant.ofEpochMilli(metadata.timestamp()));
                            }
                        }))
                .then();
    }
}
