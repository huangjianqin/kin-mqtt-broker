package org.kin.mqtt.broker.core;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.ChannelPipelineConfigurer;
import reactor.netty.ConnectionObserver;

import java.net.SocketAddress;
import java.util.Objects;

/**
 * mqtt client channel initializer
 *
 * @author huangjianqin
 * @date 2023/4/12
 */
public class MqttChannelInitializer implements ChannelPipelineConfigurer {
    private static final Logger log = LoggerFactory.getLogger(MqttChannelInitializer.class);
    /** 默认do nothing的{@link MqttChannelInitializer}实现 */
    private static final ChannelPipelineConfigurer DEFAULT = (connectionObserver, channel, remoteAddress) -> {
        //do nothing
    };

    /**
     * 创建{@link ChannelPipelineConfigurer}实例
     */
    public static ChannelPipelineConfigurer create(MqttBrokerConfig config) {
        int connectPerSec = config.getConnectPerSec();
        if (connectPerSec > 0) {
            MqttChannelInitializer initializer = new MqttChannelInitializer();
            //连接建立限流
            initializer.connectRateLimiter = RateLimiter.create(connectPerSec);
            return initializer;
        } else {
            return DEFAULT;
        }
    }

    /** 每秒连接建立限流 */
    private RateLimiter connectRateLimiter;

    private MqttChannelInitializer() {
    }

    @Override
    public void onChannelInit(ConnectionObserver connectionObserver, Channel channel, SocketAddress remoteAddress) {
        if (Objects.nonNull(connectRateLimiter) &&
                !connectRateLimiter.tryAcquire()) {
            //每秒连接建立限流
            //在channel register阶段close
            log.error("mqtt client connect too fast, reach limit {} conn/s, {}", connectRateLimiter.getRate(), remoteAddress);
            channel.close();
        }
    }
}
