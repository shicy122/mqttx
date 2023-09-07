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
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.service.ISessionService;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.service.ISysMessageService;
import com.hycan.idn.mqttx.utils.IPUtils;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;

/**
 * {@link MqttMessageType#DISCONNECT} 客户端连接断开 消息处理器
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Handler(type = MqttMessageType.DISCONNECT)
public final class DisconnectHandler extends AbstractMqttSessionHandler implements Watcher {

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
    public void onClientDisconnected(Session session) {
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

        ClientConnOrDiscMsg discMsg = ClientConnOrDiscMsg.offline(clientId, instanceId);
        if (isClusterMode()) {
            internalMessageService.publish(
                    new InternalMessage<>(discMsg, System.currentTimeMillis(), brokerId), DISCONNECT);
        }

        sysMessageService.publishSysBridge(discMsg);

        // 清理 client <-> channel 关系
        ConnectHandler.CLIENT_MAP.remove(clientId);
        if (Boolean.TRUE.equals(session.getCleanSession())) {
            ConnectHandler.ALL_CLIENT_MAP.remove(clientId);
        }

        // session 处理
        if (Boolean.TRUE.equals(session.getCleanSession())) {
            // 当 cleanSession = 1，清理会话状态。
            // MQTTX 为了提升性能，将 session/pub/pubRel 等信息保存在内存中，这部分信息关联 {@link io.netty.channel.Channel} 无需 clean 由 GC 自动回收.
            // 订阅信息则不同，此类信息通过常驻内存，需要明确调用清理的 API
            subscriptionService.clearClientSubscriptions(clientId, true)
                    .publishOn(Schedulers.boundedElastic())
                    .doOnSuccess(unused -> {
                        // 清理系统主题订阅
                        if (enableSysTopic) {
                            subscriptionService.clearClientSysSub(clientId)
                                    .publishOn(Schedulers.boundedElastic())
                                    .doOnSuccess(v -> sysMessageService.offlineNotice(discMsg).subscribe())
                                    .subscribe();
                        } else {
                            sysMessageService.offlineNotice(discMsg).subscribe();
                        }
                    })
                    .subscribe();
        } else {
            sessionService.save(session)
                    .publishOn(Schedulers.boundedElastic())
                    .doOnSuccess(unused -> sysMessageService.offlineNotice(discMsg).subscribe())
                    .subscribe();
        }
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

        Optional.ofNullable(im.getData())
                .map(ClientConnOrDiscMsg::getClientId)
                .map(ConnectHandler.CLIENT_MAP::get)
                .map(BrokerHandler.CHANNELS::find)
                .map(ChannelOutboundInvoker::close);
    }

    @Override
    public boolean support(String channel) {
        return DISCONNECT.equals(channel);
    }
}
