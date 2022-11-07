package org.kin.mqtt.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
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
        EventLoopGroup boss = new NioEventLoopGroup();
        EventLoopGroup worker = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel){
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
}
