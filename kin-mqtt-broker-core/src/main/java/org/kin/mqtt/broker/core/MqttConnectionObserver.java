package org.kin.mqtt.broker.core;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;

import java.util.Objects;

/**
 * mqtt client 连接状态listener
 *
 * @author huangjianqin
 * @date 2023/4/12
 */
public class MqttConnectionObserver implements ConnectionObserver {
    private static final Logger log = LoggerFactory.getLogger(MqttConnectionObserver.class);
    /** 默认do nothing的{@link ConnectionObserver}实现 */
    private static final ConnectionObserver DEFAULT = (connection, newState) -> {
        //do nothing
    };

    /**
     * 创建{@link MqttConnectionObserver}实例
     */
    public static ConnectionObserver create(MqttBrokerConfig config) {
        int connectPerSec = config.getConnectPerSec();
        if (connectPerSec > 0) {
            MqttConnectionObserver observer = new MqttConnectionObserver();
            //连接建立限流
            observer.connectRateLimiter = RateLimiter.create(connectPerSec);
            return observer;
        } else {
            return DEFAULT;
        }
    }

    /** 每秒连接建立限流 */
    private RateLimiter connectRateLimiter;

    private MqttConnectionObserver() {
    }

    @Override
    public void onStateChange(Connection connection, State newState) {
        if (State.CONNECTED.equals(newState) &&
                Objects.nonNull(connectRateLimiter) &&
                !connectRateLimiter.tryAcquire()) {
            //每秒连接建立限流
            Channel channel = connection.channel();
            log.error("mqtt client connect too fast, reach limit {} conn/s, {}", connectRateLimiter.getRate(), channel);
            connection.dispose();
        }
    }
}
