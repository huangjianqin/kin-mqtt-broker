package org.kin.mqtt.broker.cluster.gossip;

import org.kin.mqtt.broker.Constants;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConfigurationProperties(Constants.GOSSIP_PROPERTIES_PREFIX)
public class GossipProperties extends GossipConfig {
}
