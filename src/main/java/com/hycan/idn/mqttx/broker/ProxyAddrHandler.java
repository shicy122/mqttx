package com.hycan.idn.mqttx.broker;

import com.hycan.idn.mqttx.pojo.Session;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 获取代理IP地址Handler
 *
 * @author shichongying
 * @datetime 2023-11-01 09:12
 */
@Slf4j
@ChannelHandler.Sharable
@Component
public class ProxyAddrHandler extends ChannelInboundHandlerAdapter {

    private static final String PROXY_TCP4_PREFIX = "PROXY TCP4 ";

    public static final String PROXY_ADDR = "proxyAddr";

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)  {
        Session session = (Session) ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get();
        if (session == null) {
            if (msg instanceof ByteBuf buf) {
                String dataStr = buf.toString(CharsetUtil.UTF_8);
                if (dataStr.startsWith(PROXY_TCP4_PREFIX)) {
                    int proxyAddrEndIndex = dataStr.indexOf("\r\n") + 2;
                    String[] parts = dataStr.substring(PROXY_TCP4_PREFIX.length(), proxyAddrEndIndex).split(" ");
                    String proxyAddr = parts[0] + ":" + parts[2];
                    ctx.channel().attr(AttributeKey.valueOf(PROXY_ADDR)).set(proxyAddr);

                    // 将 PROXY 部分的数据消耗掉
                    buf.readerIndex(proxyAddrEndIndex);
                    ByteBuf payload = buf.readBytes(buf.readableBytes());
                    // 释放 buf 对象
                    buf.release();
                    // 将剩余的消息发送到下一个 ChannelHandler
                    ctx.fireChannelRead(payload);
                    return;
                }
            }
        }

        // 将消息继续传递给下一个 ChannelHandler
        ctx.fireChannelRead(msg);
    }
}