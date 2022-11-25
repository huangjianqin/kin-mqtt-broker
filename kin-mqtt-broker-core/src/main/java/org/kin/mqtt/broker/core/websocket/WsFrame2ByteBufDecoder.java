package org.kin.mqtt.broker.core.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.util.List;

/**
 * @author huangjianqin
 * @date 2022/11/25
 */
public final class WsFrame2ByteBufDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, BinaryWebSocketFrame wsFrame, List<Object> out) {
        ByteBuf buf = wsFrame.content();
        buf.retain();
        out.add(buf);
    }
}