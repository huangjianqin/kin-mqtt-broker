package org.kin.mqtt.broker.cluster.standalone;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(MqttBrokerMarkerConfiguration.class)
public @interface EnableMqttBroker {
}
