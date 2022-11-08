package org.kin.mqtt.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2022/11/6
 */
@Configuration
@EnableConfigurationProperties(MqttBrokerProperties.class)
public class MqttBrokerAutoConfiguration {
    @Autowired
    private MqttBrokerProperties mqttBrokerProperties;

    /**
     * start MQTT server
     *
     * @return spring {@link DisposableBean}
     * @throws Exception exception
     */
    @Bean
    public DisposableBean mqttServerDisposable() throws Exception {
        EventLoopGroup boss = getAdaptiveEventLoopGroup(2);
        EventLoopGroup worker = getAdaptiveEventLoopGroup(0);
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boss, worker)
                .channel(getAdaptiveServerChannelClass())
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        socketChannel.pipeline().addLast(new MqttDecoder());
                        socketChannel.pipeline().addLast(MqttEncoder.INSTANCE);
                        socketChannel.pipeline().addLast("MqttServerHandler", new MqttServerHandler());
                    }
                });
        serverBootstrap.bind(mqttBrokerProperties.getPort()).sync();
        return () -> {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        };
    }

    /**
     * 根据部署环境获取合适的{@link EventLoopGroup}实现, nio or epoll
     */
    private static EventLoopGroup getAdaptiveEventLoopGroup(int nThreads) {
        nThreads = Math.max(nThreads, 0);
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(nThreads);
        }
        return new NioEventLoopGroup(nThreads);
    }

    /**
     * 根据部署环境获取合适的{@link ServerChannel}实现, nio or epoll
     */
    private static Class<? extends ServerChannel> getAdaptiveServerChannelClass() {
        if (Epoll.isAvailable()) {
            return EpollServerSocketChannel.class;
        }
        return NioServerSocketChannel.class;
    }
}
