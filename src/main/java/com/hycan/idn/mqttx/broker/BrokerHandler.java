/*
 * Copyright 2002-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hycan.idn.mqttx.broker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.broker.handler.DisconnectHandler;
import com.hycan.idn.mqttx.broker.handler.MessageDelegatingHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.consumer.Watcher;
import com.hycan.idn.mqttx.exception.AuthorizationException;
import com.hycan.idn.mqttx.pojo.Authentication;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.IAuthenticationService;
import com.hycan.idn.mqttx.service.IInstanceRouterService;
import com.hycan.idn.mqttx.service.ISessionService;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * broker handler
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@ChannelHandler.Sharable
@Component
public class BrokerHandler extends SimpleChannelInboundHandler<MqttMessage> implements Watcher {
    //@formatter:off
    /** channel 群组 */
    public static final ChannelGroup CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    /** 历史最大连接数量 */
    public static final AtomicInteger MAX_ACTIVE_SIZE = new AtomicInteger(0);
    /** broker 启动时间 */
    public static final long START_TIME = System.currentTimeMillis();
    private final MessageDelegatingHandler messageDelegatingHandler;
    private final DisconnectHandler disconnectHandler;
    private final ISubscriptionService subscriptionService;
    private final IAuthenticationService authenticationService;
    private final IInstanceRouterService instanceRouterService;

    private final Serializer serializer;
    private final boolean enableSysTopic;
    private final String AUTHORIZED;

    //@formatter:on

    public BrokerHandler(MqttxConfig config,
                         MessageDelegatingHandler messageDelegatingHandler,
                         ISessionService sessionService,
                         DisconnectHandler disconnectHandler,
                         ISubscriptionService subscriptionService,
                         IAuthenticationService authenticationService,
                         IInstanceRouterService instanceRouterService, Serializer serializer) {
        this.disconnectHandler = disconnectHandler;
        this.authenticationService = authenticationService;
        this.instanceRouterService = instanceRouterService;
        Assert.notNull(messageDelegatingHandler, "messageDelegatingHandler can't be null");
        Assert.notNull(sessionService, "sessionService can't be null");
        Assert.notNull(subscriptionService, "subscriptionService can't be null");
        Assert.notNull(serializer, "serializer can't be null");

        MqttxConfig.SysTopic sysTopic = config.getSysTopic();
        this.messageDelegatingHandler = messageDelegatingHandler;
        this.subscriptionService = subscriptionService;
        this.serializer = serializer;
        this.enableSysTopic = sysTopic.getEnable();
        this.AUTHORIZED = config.getKafka().getAuthorized();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        CHANNELS.add(ctx.channel());

        if (enableSysTopic) {
            // cas
            while (true) {
                int old = MAX_ACTIVE_SIZE.get();
                int now = CHANNELS.size();

                if (old >= now) {
                    break;
                }

                if (MAX_ACTIVE_SIZE.compareAndSet(old, now)) {
                    break;
                }
            }
        }
    }

    /**
     * 连接断开后进行如下操作:
     * <ol>
     *     <li>清理 {@link ConnectHandler#CLIENT_MAP} 中保存的 clientId 与 channelId 绑定关系</li>
     *     <li>遗嘱消息处理</li>
     *     <li>当 cleanSession = 0 时持久化 session,这样做的目的是保存 <code>Session#messageId</code> 字段变化</li>
     * </ol>
     * <p>
     * [MQTT-3.1.2-8]
     * If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be
     * stored on the Server and associated with the Network Connection. The Will Message MUST be published when the
     * Network Connection is subsequently closed unless the Will Message has been deleted by the Server on receipt of
     * a DISCONNECT Packet.
     *
     * @param ctx {@link ChannelHandlerContext}
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 获取当前会话
        Session session = (Session) ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get();

        // 会话状态处理
        if (session != null) {
            disconnectHandler.onClientDisconnected(session);
            instanceRouterService.clearAdminClientConnect(session.getClientId()).subscribe();
            log.info("断开连接: 客户端ID=[{}], channel=[{}]", session.getClientId(), ctx.channel().id());
        }
        ctx.close();
    }

    /**
     * 异常处理及请求分发
     *
     * @param ctx         {@link ChannelHandlerContext}
     * @param mqttMessage {@link MqttMessage}
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        // 异常处理
        if (mqttMessage.decoderResult().isFailure()) {
            exceptionCaught(ctx, mqttMessage.decoderResult().cause());
            return;
        }

        // 消息处理
        messageDelegatingHandler.handle(ctx, mqttMessage);
    }

    /**
     * 异常处理
     *
     * @param ctx   {@link ChannelHandlerContext}
     * @param cause 异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 主要处理 connect 消息相关异常
        MqttConnAckMessage mqttConnAckMessage = null;
        if (cause instanceof MqttIdentifierRejectedException) {
            mqttConnAckMessage = MqttMessageBuilders.connAck()
                    .sessionPresent(false)
                    .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED)
                    .build();
            log.error("MQTT客户端标识异常=[{}], channel=[{}]", cause.getMessage(), ctx.channel().id());
        } else if (cause instanceof MqttUnacceptableProtocolVersionException) {
            mqttConnAckMessage = MqttMessageBuilders.connAck()
                    .sessionPresent(false)
                    .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION)
                    .build();
            log.error("MQTT协议版本异常=[{}], channel=[{}]", cause.getMessage(), ctx.channel().id());
        } else if (cause instanceof AuthorizationException) {
            mqttConnAckMessage = MqttMessageBuilders.connAck()
                    .sessionPresent(false)
                    .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED)
                    .build();
            Session session = (Session) ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get();
            if (session != null) {
                ConnectHandler.CLIENT_MAP.remove(session.getClientId());
                log.error("客户端[{}]权限异常=[{}], channel=[{}]", session.getClientId(), cause.getMessage(), ctx.channel().id());
            } else {
                log.error("客户端权限异常=[{}], channel=[{}]", cause.getMessage(), ctx.channel().id());
            }
        } else if (cause instanceof IOException) {
            // 连接被强制断开
            Session session = (Session) ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get();
            if (session != null) {
                ConnectHandler.CLIENT_MAP.remove(session.getClientId());
                log.warn("连接被强制断开: 客户端ID=[{}], channel=[{}]", session.getClientId(), ctx.channel().id());
            } else {
                log.error("连接被强制断开: 异常=[{}], channel=[{}]", cause.getMessage(), ctx.channel().id());
            }
        } else {
            Session session = (Session) ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get();
            if (session != null) {
                ConnectHandler.CLIENT_MAP.remove(session.getClientId());
                log.error("未知异常=[{}], 客户端ID=[{}], channel=[{}]", cause.getMessage(), session.getClientId(), ctx.channel().id());
            } else {
                log.error("未知异常=[{}], channel=[{}]", cause.getMessage(), ctx.channel().id());
            }
        }

        if (mqttConnAckMessage != null) {
            ctx.writeAndFlush(mqttConnAckMessage);
        }
        ctx.close();
    }

    /**
     * 心跳、握手事件处理
     *
     * @param ctx {@link ChannelHandlerContext}
     * @param evt {@link IdleStateEvent}
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        Channel channel = ctx.channel();
        Session session = (Session) channel.attr(AttributeKey.valueOf(Session.KEY)).get();
        if (Objects.isNull(session)) {
            log.warn("Session is null, channel id is {}", channel.id());
            return;
        }
        String clientId = session.getClientId();

        InetSocketAddress socketAddress = (InetSocketAddress) channel.remoteAddress();
        if (Objects.isNull(socketAddress)) {
            log.warn("Socket address is null, channel id is {}", channel.id());
            return;
        }
        String host = socketAddress.getAddress().getHostAddress();
        int port = socketAddress.getPort();

        if (evt instanceof IdleStateEvent ise) {
            if (IdleState.ALL_IDLE.equals(ise.state())) {
                log.warn("心跳超时: 客户端ID=[{}], channel=[{}], R[{}:{}]", clientId, channel.id(), host, port);

                // 关闭连接
                ctx.close();
            }
        } else if (evt instanceof SslHandshakeCompletionEvent shce) {
            // 监听 ssl 握手事件
            if (!shce.isSuccess()) {
                log.warn("SSL握手失败: 客户端ID={}, R[{}:{}]", clientId, host, port);
                ctx.close();
            }
        }
    }

    /**
     * 行为：
     * <ol>
     *     <li>修改用户权限</li>
     * </ol>
     *
     * @param msg 集群消息
     */
    @Override
    public void action(byte[] msg) {
        InternalMessage<Authentication> im;
        if (serializer instanceof JsonSerializer s) {
            im = s.deserialize(msg, new TypeReference<>() {
            });
        } else {
            //noinspection unchecked
            im = serializer.deserialize(msg, InternalMessage.class);
        }

        Authentication data = im.getData();
        // 目的是为了兼容 v1.0.2(含) 之前的版本
        String clientId = data.getClientId();
        List<String> authorizedPub = data.getAuthorizedPub();
        List<String> authorizedSub = data.getAuthorizedSub();
        if (ObjectUtils.isEmpty(clientId) || (CollectionUtils.isEmpty(authorizedPub) && CollectionUtils.isEmpty(authorizedSub))) {
            log.warn("权限修改参数非法:{}", im);
            return;
        }
        authenticationService.alterUserAuthorizedTopic(clientId, authorizedSub, authorizedPub);

        // 移除 cache&redis 中客户端订阅的 topic
        if (!CollectionUtils.isEmpty(authorizedSub)) {
            subscriptionService.clearUnAuthorizedClientSub(clientId, authorizedSub).subscribe();
        }
    }

    @Override
    public boolean support(String channel) {
        return AUTHORIZED.equals(channel);
    }
}
