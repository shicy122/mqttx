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

package com.hycan.idn.mqttx.broker.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.consumer.Watcher;
import com.hycan.idn.mqttx.pojo.ClientConnOrDiscMsg;
import com.hycan.idn.mqttx.pojo.ConnectInfo;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.service.ISessionService;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.service.ISysMessageService;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.hycan.idn.mqttx.graceful.CurrentMqttxStateListener.IS_INSTANCE_DOWN_EVENT;

/**
 * {@link MqttMessageType#DISCONNECT} 客户端连接断开 消息处理器
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Handler(type = MqttMessageType.DISCONNECT)
public final class DisconnectHandler extends AbstractMqttSessionHandler implements Watcher {

    private static final String IS_CLUSTER_DISC_MSG = "isClusterDiscMsg";

    private final Serializer serializer;
    private final ISessionService sessionService;
    private final PublishHandler publishHandler;
    private final ISubscriptionService subscriptionService;
    private final IInternalMessageService internalMessageService;
    private final ISysMessageService sysMessageService;

    private final String brokerId, instanceId, DISCONNECT;
    private final boolean enableSysTopic;

    public DisconnectHandler(MqttxConfig config,
                             Serializer serializer,
                             ISessionService sessionService,
                             PublishHandler publishHandler,
                             ISubscriptionService subscriptionService,
                             IInternalMessageService internalMessageService,
                             ISysMessageService sysMessageService) {
        super(config.getCluster().getEnable());
        this.serializer = serializer;
        this.sessionService = sessionService;
        this.publishHandler = publishHandler;
        this.subscriptionService = subscriptionService;

        this.brokerId = config.getBrokerId();
        this.instanceId = config.getInstanceId();
        this.enableSysTopic = config.getSysTopic().getEnable();
        this.internalMessageService = internalMessageService;
        this.sysMessageService = sysMessageService;
        this.DISCONNECT = config.getKafka().getDisconnect();
    }

    /**
     * 处理 disconnect 报文
     *
     * @param ctx 见 {@link ChannelHandlerContext}
     * @param msg 解包后的数据
     */
    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {
        // [MQTT-3.1.2-8]
        // If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be
        // stored on the Server and associated with the Network Connection. The Will Message MUST be published when the
        // Network Connection is subsequently closed unless the Will Message has been deleted by the Server on receipt of
        // a DISCONNECT Packet.
        Session session = getSession(ctx);
        session.clearWillMessage();

        ctx.close();
    }

    /**
     * 客户端下线处理:
     * <ol>
     *     <li>遗嘱消息</li>
     *     <li>{@link Session} 处理</li>
     *     <li>下线通知</li>
     * </ol>
     *
     * @param session 会话
     */
    public void onClientDisconnected(Session session, Channel channel) {
        final String clientId = session.getClientId();

        // 发布遗嘱消息
        Optional.of(session)
                .map(Session::getWillMessage)
                .ifPresent(msg -> {
                    // 解决遗嘱消息无法 retain 的 bug
                    if (msg.isRetain()) {
                        publishHandler.handleRetainMsg(msg)
                                .then(publishHandler.publish(msg, session, false))
                                .subscribe();
                    } else {
                        publishHandler.publish(msg, session, false).subscribe();
                    }
                });


        // 清理 client <-> channel 关系
        ConnectHandler.CLIENT_MAP.remove(clientId);
        // 集群消息不清理 client <-> 连接点 关系，在集群消息中已经清理过一次
        if (Objects.isNull(channel.attr(AttributeKey.valueOf(IS_CLUSTER_DISC_MSG)).get())) {
            ConnectHandler.ALL_CLIENT_MAP.remove(clientId);
        }

        // session 处理
        if (Boolean.TRUE.equals(session.getCleanSession())) {
            // 当 cleanSession = 1，清理会话状态。
            // MQTTX 为了提升性能，将 session/pub/pubRel 等信息保存在内存中，这部分信息关联 {@link io.netty.channel.Channel} 无需 clean 由 GC 自动回收.
            // 订阅信息则不同，此类信息通过常驻内存，需要明确调用清理的 API
            Object isInstanceDownEvent = channel.attr(AttributeKey.valueOf(IS_INSTANCE_DOWN_EVENT)).get();
            boolean isPublishSubClusterMessage = Objects.isNull(isInstanceDownEvent);
            subscriptionService.clearClientSubscriptions(clientId, true, isPublishSubClusterMessage)
                    .publishOn(Schedulers.boundedElastic())
                    .doOnSuccess(unused -> {
                        // 清理系统主题订阅
                        if (enableSysTopic) {
                            subscriptionService.clearClientSysSub(clientId, isPublishSubClusterMessage)
                                    .publishOn(Schedulers.boundedElastic())
                                    .doOnSuccess(v -> publishDiscMsg(channel, clientId))
                                    .subscribe();
                        } else {
                            publishDiscMsg(channel, clientId);
                        }
                    })
                    .subscribe();
        } else {
            sessionService.save(session)
                    .publishOn(Schedulers.boundedElastic())
                    .doOnSuccess(unused -> publishDiscMsg(channel, clientId))
                    .subscribe();
        }
    }

    /**
     * 发送断开连接消息通知
     *
     * @param channel  {@link Channel}
     * @param clientId 客户端 id
     */
    private void publishDiscMsg(Channel channel, String clientId) {
        ClientConnOrDiscMsg discMsg = ClientConnOrDiscMsg.offline(clientId, channel.id(), instanceId);

        // 集群消息触发的断开连接，不需要再次向集群发送断开通知
        Object isClusterDiscMsg = channel.attr(AttributeKey.valueOf(IS_CLUSTER_DISC_MSG)).get();
        // 实例下线时，不发送断开连接消息
        Object isInstanceDownEvent = channel.attr(AttributeKey.valueOf(IS_INSTANCE_DOWN_EVENT)).get();
        if (isClusterMode() && Objects.isNull(isClusterDiscMsg) && Objects.isNull(isInstanceDownEvent)) {
            internalMessageService.publish(
                    new InternalMessage<>(discMsg, System.currentTimeMillis(), brokerId), DISCONNECT);
        }

        // 发送断开连接消息到Kafka
        sysMessageService.publishSysBridge(discMsg);

        // 发送断开连接消息到Mqtt client
        sysMessageService.offlineNotice(discMsg).subscribe();
    }

    @Override
    public void action(byte[] msg) {
        InternalMessage<ClientConnOrDiscMsg> im;
        if (serializer instanceof JsonSerializer se) {
            im = se.deserialize(msg, new TypeReference<>() {});
        } else {
            //noinspection unchecked
            im = serializer.deserialize(msg, InternalMessage.class);
        }

        ClientConnOrDiscMsg imMsg = im.getData();
        if (Objects.isNull(imMsg)) {
            return;
        }

        String clientId = imMsg.getClientId();

        /*
         * 断开连接时，清理ALL_CLIENT_MAP中的缓存，当连接成功时间 < 断开连接时间 ，则删除缓存
         * 启动连接场景，会先发送一条断开连接集群消息，再发送一条连接成功集群消息，这两条消息会在非常短的时间间隔内发出
         * 连接成功集群消息，对应的时间戳默认在当前时间 +150ms，目的是防止并发时，ALL_CLIENT_MAP中的缓存维护错乱
         */
        ConnectHandler.ALL_CLIENT_MAP.computeIfPresent(clientId, (key, value) ->
                value.getTimestamp() < imMsg.getTimestamp() ? null : value);

        log.info("集群消息-断开连接: 客户端ID=[{}], 连接当前节点=[{}], 缓存连接关系=[{}]",
                clientId, ConnectHandler.CLIENT_MAP.containsKey(clientId),
                ConnectHandler.ALL_CLIENT_MAP.containsKey(clientId));

        Optional.ofNullable(ConnectHandler.CLIENT_MAP.get(clientId))
                .map(BrokerHandler.CHANNELS::find)
                .map(channel -> {
                    channel.attr(AttributeKey.valueOf(IS_CLUSTER_DISC_MSG)).set(true);
                    return channel;
                })
                .ifPresent(ChannelOutboundInvoker::close);
    }

    @Override
    public boolean support(String channel) {
        return DISCONNECT.equals(channel);
    }
}
