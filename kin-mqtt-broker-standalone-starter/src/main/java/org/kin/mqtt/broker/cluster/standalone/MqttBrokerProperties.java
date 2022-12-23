package org.kin.mqtt.broker.cluster.standalone;

import org.kin.mqtt.broker.Constants;
import org.kin.mqtt.broker.core.MqttBrokerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConfigurationProperties(Constants.COMMON_PROPERTIES_PREFIX)
public class MqttBrokerProperties extends MqttBrokerConfig {
}
