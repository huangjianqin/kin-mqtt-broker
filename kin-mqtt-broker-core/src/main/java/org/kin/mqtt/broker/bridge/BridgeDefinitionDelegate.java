package org.kin.mqtt.broker.bridge;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.kin.mqtt.broker.bridge.definition.BridgeDefinition;
import org.kin.mqtt.broker.bridge.definition.HttpBridgeDefinition;
import org.kin.mqtt.broker.bridge.definition.KafkaBridgeDefinition;
import org.kin.mqtt.broker.bridge.definition.RabbitMQBridgeDefinition;

import java.io.Serializable;

/**
 * @author huangjianqin
 * @date 2023/5/26
 */
public class BridgeDefinitionDelegate implements Serializable {
    private static final long serialVersionUID = -3181798534292546287L;
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({@JsonSubTypes.Type(name = "http", value = HttpBridgeDefinition.class),
            @JsonSubTypes.Type(name = "kafka", value = KafkaBridgeDefinition.class),
            @JsonSubTypes.Type(name = "rabbitMQ", value = RabbitMQBridgeDefinition.class)})
    private BridgeDefinition delegate;

    public BridgeDefinitionDelegate() {
    }

    public BridgeDefinitionDelegate(BridgeDefinition delegate) {
        this.delegate = delegate;
    }

    //setter && getter
    public BridgeDefinition getDelegate() {
        return delegate;
    }

    public void setDelegate(BridgeDefinition delegate) {
        this.delegate = delegate;
    }
}
