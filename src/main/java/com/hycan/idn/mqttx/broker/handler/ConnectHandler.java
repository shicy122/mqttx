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
import com.hycan.idn.mqttx.broker.ProxyAddrHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.consumer.Watcher;
import com.hycan.idn.mqttx.exception.AuthenticationException;
import com.hycan.idn.mqttx.pojo.Authentication;
import com.hycan.idn.mqttx.pojo.ClientAuth;
import com.hycan.idn.mqttx.pojo.ClientConnOrDiscMsg;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.IAuthenticationService;
import com.hycan.idn.mqttx.service.IInstanceRouterService;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.service.IPubRelMessageService;
import com.hycan.idn.mqttx.service.IPublishMessageService;
import com.hycan.idn.mqttx.service.ISessionService;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.service.ISysMessageService;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link io.netty.handler.codec.mqtt.MqttMessageType#CONNECT} 客户端连接 消息处理器
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Handler(type = MqttMessageType.CONNECT)
public final class ConnectHandler extends AbstractMqttTopicSecureHandler implements Watcher {
    //@formatter:off

    public static final ConcurrentHashMap<String, ChannelId> CLIENT_MAP = new ConcurrentHashMap<>(10000);
    public static final ConcurrentHashMap<String, String> ALL_CLIENT_MAP = new ConcurrentHashMap<>(10000);
    private static final String NONE_ID_PREFIX = "NONE_ID_";
    private final Serializer serializer;
    private final boolean enableTopicSubPubSecure;
    private final String brokerId, currentInstanceId, CONNECT, AUTHORIZED;
    private final Boolean enableAuth, enableLog;
    private final List<String> adminUser;
    private final int maxConnectSize;
    private final List<String> adminClientIdPrefix;

    /**
     * 认证服务
     */
    private final IAuthenticationService authenticationService;
    /**
     * 会话服务
     */
    private final ISessionService sessionService;
    /**
     * 主题订阅相关服务
     */
    private final ISubscriptionService subscriptionService;
    /**
     * publish 消息服务
     */
    private final IPublishMessageService publishMessageService;
    /**
     * pubRel 消息服务
     */
    private final IPubRelMessageService pubRelMessageService;
    /**
     * 内部消息发布服务
     */
    private final IInternalMessageService internalMessageService;
    /**
     * 连接业务
     */
    private final ISysMessageService sysMessageService;

    private final IInstanceRouterService instanceRouterService;

    //@formatter:on

    public ConnectHandler(Serializer serializer,
                          IAuthenticationService authenticationService,
                          ISessionService sessionService,
                          ISubscriptionService subscriptionService,
                          IPublishMessageService publishMessageService,
                          IPubRelMessageService pubRelMessageService,
                          MqttxConfig config,
                          @Nullable IInternalMessageService internalMessageService,
                          ISysMessageService sysMessageService,
                          IInstanceRouterService instanceRouterService) {
        super(config.getCluster().getEnable());
        this.serializer = serializer;
        this.sysMessageService = sysMessageService;
        this.instanceRouterService = instanceRouterService;
        Assert.notNull(authenticationService, "authentication can't be null");
        Assert.notNull(sessionService, "sessionService can't be null");
        Assert.notNull(subscriptionService, "subscriptionService can't be null");
        Assert.notNull(publishMessageService, "publishMessageService can't be null");
        Assert.notNull(pubRelMessageService, "pubRelMessageService can't be null");
        Assert.notNull(config, "mqttxConfig can't be null");
        Assert.notNull(internalMessageService, "internalMessageService can't be null");

        this.authenticationService = authenticationService;
        this.sessionService = sessionService;
        this.subscriptionService = subscriptionService;
        this.publishMessageService = publishMessageService;
        this.pubRelMessageService = pubRelMessageService;
        this.internalMessageService = internalMessageService;

        MqttxConfig.Auth auth = config.getAuth();

        this.brokerId = config.getBrokerId();
        this.currentInstanceId = config.getInstanceId();
        this.enableAuth = auth.getEnable();
        this.enableLog = config.getSysConfig().getEnableLog();
        this.adminUser = auth.getAdminUser();
        this.maxConnectSize = config.getSysConfig().getMaxConnectSize();
        this.enableTopicSubPubSecure = config.getFilterTopic().getEnableTopicSubPubSecure();
        this.CONNECT = config.getKafka().getConnect();
        this.AUTHORIZED = config.getKafka().getAuthorized();
        this.adminClientIdPrefix = config.getAuth().getAdminClientIdPrefix();
    }

    /**
     * 客户端连接请求处理
     *
     * @param ctx 见 {@link ChannelHandlerContext}
     * @param msg 解包后的数据
     */
    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {
        // 获取identifier,password
        MqttConnectMessage mcm = (MqttConnectMessage) msg;
        MqttConnectVariableHeader variableHeader = mcm.variableHeader();
        MqttConnectPayload payload = mcm.payload();
        final String username = payload.userName();
        final byte[] password = payload.passwordInBytes();
        var clientIdentifier = payload.clientIdentifier();
        // 连接数量超过配置中的最大连接数时，拒绝连接
        if (maxConnectSize >= 0 && CLIENT_MAP.size() >= maxConnectSize) {
            log.error("当前节点的连接数量超过限制, 限制大小=[{}]", maxConnectSize);
            return;
        }

        if (ObjectUtils.isEmpty(clientIdentifier)) {
            // [MQTT-3.1.3-8] If the Client supplies a zero-byte ClientId with CleanSession set to 0, the Server MUST
            // respond to the CONNECT Packet with a CONNACK return code 0x02 (Identifier rejected) and then close the
            // Network Connection.
            if (!variableHeader.isCleanSession()) {
                throw new MqttIdentifierRejectedException("Violation: zero-byte ClientId with CleanSession set to 0");
            }

            // broker  生成一个唯一ID
            // [MQTT-3.1.3-6] A Server MAY allow a Client to supply a ClientId that has a length of zero bytes,
            // however if it does so the Server MUST treat this as a special case and assign a unique ClientId to that Client.
            // It MUST then process the CONNECT packet as if the Client had provided that unique ClientId
            clientIdentifier = genClientId();
        }
        final var clientId = clientIdentifier; // for lambda
        log.info("启动连接: 客户端ID=[{}], channel=[{}], R[{}]", clientId, ctx.channel().id(), clientAddress(ctx));

        ClientAuth clientAuth = ClientAuth.of(clientId, username, password);

        if (!CollectionUtils.isEmpty(adminUser) && (adminUser.contains(username)
                || adminClientIdPrefix.stream().filter(clientId::startsWith).anyMatch(list -> true))) {
            authenticationService.adminAuthenticate(clientAuth)
                    .flatMap(returnCode -> {
                        if (MqttConnectReturnCode.CONNECTION_ACCEPTED.equals(returnCode)) {
                            process0(ctx, msg, Authentication.of(clientId,
                                    Collections.singletonList(TopicUtils.ALL_TOPIC),
                                    Collections.singletonList(TopicUtils.ALL_TOPIC)));
                            return Mono.empty();
                        }

                        MqttConnAckMessage mqttConnAckMessage = MqttMessageBuilders.connAck()
                                .sessionPresent(false)
                                .returnCode(returnCode)
                                .build();
                        ctx.writeAndFlush(mqttConnAckMessage);
                        ctx.close();
                        return Mono.empty();
                    }).subscribe();
            return;
        }

        if (Boolean.TRUE.equals(enableAuth)) {
            if (!variableHeader.hasPassword() || !variableHeader.hasUserName()) {
                MqttConnAckMessage mqttConnAckMessage = MqttMessageBuilders.connAck()
                        .sessionPresent(false)
                        .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD)
                        .build();
                log.error(String.format("client[id: %s, username: %s]登入失败", clientId, username));

                // 告知客户端并关闭连接
                ctx.writeAndFlush(mqttConnAckMessage);
                ctx.close();
                return;
            }

            authenticationService.asyncAuthenticate(clientAuth)
                    .thenAccept(authentication -> {
                        if (authentication == null) {
                            // authentication 是有可能为 null 的
                            // 比如 认证服务 response http status = 200, 但是响应内容为空
                            authentication = Authentication.of(clientId);
                        } else {
                            authentication.setClientId(clientId); // 置入 clientId
                        }
                        if (Boolean.TRUE.equals(enableLog)) {
                            log.info("鉴权成功: 客户端ID=[{}], 授权发布主题列表={}, 授权订阅主题列表={}",
                                    clientId, authentication.getAuthorizedPub(), authentication.getAuthorizedSub());
                        }

                        process0(ctx, msg, authentication);
                    })
                    .exceptionally(throwable -> {
                        MqttConnAckMessage mqttConnAckMessage;
                        if (throwable.getCause() instanceof AuthenticationException) {
                            mqttConnAckMessage = MqttMessageBuilders.connAck()
                                    .sessionPresent(false)
                                    .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD)
                                    .build();
                        } else {
                            mqttConnAckMessage = MqttMessageBuilders.connAck()
                                    .sessionPresent(false)
                                    .returnCode(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                                    .build();
                        }
                        log.error(String.format("client[id: %s, username: %s]登入失败", clientId, username), throwable);

                        // 告知客户端并关闭连接
                        ctx.writeAndFlush(mqttConnAckMessage);
                        ctx.close();
                        return null;
                    });
        } else {
            process0(ctx, msg, Authentication.of(clientId));
        }
    }

    /**
     * 客户端连接请求处理，流程如下：
     * <ol>
     *     <li>处理 clientId 关联的连接 </li>
     *     <li>返回响应报文</li>
     *     <li>心跳检测</li>
     *     <li>Qos1\Qos2 消息处理</li>
     * </ol>
     *
     * @param ctx  见 {@link ChannelHandlerContext}
     * @param msg  解包后的数据
     * @param auth 认证对象，见 {@link Authentication}, 该对象可能不允许为空
     */
    private void process0(ChannelHandlerContext ctx, MqttMessage msg, Authentication auth) {
        final var mcm = (MqttConnectMessage) msg;
        final var variableHeader = mcm.variableHeader();
        final var payload = mcm.payload();
        final var channel = ctx.channel();

        // 获取clientId
        final var clientId = auth.getClientId();

        // 关闭之前可能存在的tcp链接
        // [MQTT-3.1.4-2] If the ClientId represents a Client already connected to the Server then the Server MUST
        // disconnect the existing Client
        if (CLIENT_MAP.containsKey(clientId)) {
            Optional.ofNullable(CLIENT_MAP.get(clientId))
                    .map(BrokerHandler.CHANNELS::find)
                    .filter(c -> !Objects.equals(c, channel))
                    .ifPresent(ChannelOutboundInvoker::close);
        }

        ClientConnOrDiscMsg connMsg = ClientConnOrDiscMsg.online(clientId, currentInstanceId);
        if (isClusterMode()) {
            internalMessageService.publish(
                    new InternalMessage<>(connMsg, System.currentTimeMillis(), brokerId), CONNECT);
        }

        // 会话状态的处理
        // [MQTT-3.1.3-7] If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1. -
        // 这部分是 client 遵守的规则
        // [MQTT-3.1.2-6] State data associated with this Session MUST NOT be reused in any subsequent Session - 针对
        // clearSession == 1 的情况，需要清理之前保存的会话状态
        var isCleanSession = variableHeader.isCleanSession();
        if (isCleanSession) {
            actionOnCleanSession(clientId)
                    .publishOn(Schedulers.boundedElastic())
                    .doOnSuccess(unused -> {
                        // 新建会话并保存会话，同时判断sessionPresent
                        final var session = Session.of(clientId, true);
                        CLIENT_MAP.put(clientId, channel.id());
                        ALL_CLIENT_MAP.put(clientId, currentInstanceId);
                        saveSessionWithChannel(ctx, session);
                        if (enableTopicSubPubSecure) {
                            saveAuthorizedTopics(ctx, auth);
                            if (isClusterMode()) {
                                internalMessageService.publish(
                                        new InternalMessage<>(auth, System.currentTimeMillis(), brokerId), AUTHORIZED);
                            }
                        }

                        // 处理遗嘱消息
                        // [MQTT-3.1.2-8] If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will
                        // Message MUST be stored on the Server and associated with the Network Connection. The Will Message MUST be
                        // published when the Network Connection is subsequently closed unless the Will Message has been deleted by the
                        // Server on receipt of a DISCONNECT Packet.
                        boolean willFlag = variableHeader.isWillFlag();
                        if (willFlag) {
                            var pubMsg = PubMsg.of(variableHeader.willQos(), payload.willTopic(),
                                    variableHeader.isWillRetain(), payload.willMessageInBytes());
                            pubMsg.setWill(true);
                            session.setWillMessage(pubMsg);
                        }

                        // 返回连接响应
                        var acceptAck = MqttMessageBuilders.connAck()
                                .sessionPresent(false)
                                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                                .build();
                        ctx.writeAndFlush(acceptAck);

                        // 连接成功相关处理
                        sysMessageService.onlineNotice(connMsg)
                                .doOnError(t -> log.error(t.getMessage(), t))
                                .subscribe();
                        sysMessageService.publishSysBridge(connMsg);

                        if (!channel.isActive()) {
                            instanceRouterService.clearAdminClientConnect(session.getClientId()).subscribe();
                            log.warn("会话失效: 客户端ID=[{}], channel=[{}]", clientId, channel.id().asShortText());
                            return;
                        }

                        // 心跳超时设置
                        // [MQTT-3.1.2-24] If the Keep Alive value is non-zero and the Server does not receive a Control Packet from
                        // the Client within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection
                        // to the Client as if the network had failed
                        double heartbeat = variableHeader.keepAliveTimeSeconds() * 1.5;
                        if (heartbeat > 0) {
                            // 替换掉 NioChannelSocket 初始化时加入的 idleHandler
                            ctx.pipeline()
                                    .replace(IdleStateHandler.class, "idleHandler", new IdleStateHandler(
                                            0, 0, (int) heartbeat));
                        }
                        log.info("连接成功: 客户端ID=[{}], channel=[{}]", clientId, channel.id());
                    })
                    .doOnError(t -> log.error(t.getMessage(), t))
                    .subscribe();
        } else {
            // 新建会话并保存会话，同时判断sessionPresent
            sessionService.find(clientId)
                    .publishOn(Schedulers.boundedElastic())
                    .doOnSuccess(s -> {
                        Session session = s;
                        boolean sessionPresent = false;
                        if (session == null) {
                            session = Session.of(clientId, false);
                        } else {
                            sessionPresent = true;
                        }

                        CLIENT_MAP.put(clientId, ctx.channel().id());
                        ALL_CLIENT_MAP.put(clientId, currentInstanceId);
                        saveSessionWithChannel(ctx, session);
                        if (enableTopicSubPubSecure) {
                            saveAuthorizedTopics(ctx, auth);
                            if (isClusterMode()) {
                                internalMessageService.publish(
                                        new InternalMessage<>(auth, System.currentTimeMillis(), brokerId), AUTHORIZED);
                            }
                        }

                        // 处理遗嘱消息
                        // [MQTT-3.1.2-8] If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will
                        // Message MUST be stored on the Server and associated with the Network Connection. The Will Message MUST be
                        // published when the Network Connection is subsequently closed unless the Will Message has been deleted by the
                        // Server on receipt of a DISCONNECT Packet.
                        var willFlag = variableHeader.isWillFlag();
                        if (willFlag) {
                            var pubMsg = PubMsg.of(variableHeader.willQos(), payload.willTopic(),
                                    variableHeader.isWillRetain(), payload.willMessageInBytes());
                            pubMsg.setWill(true);
                            session.setWillMessage(pubMsg);
                        }

                        // 返回连接响应
                        var acceptAck = MqttMessageBuilders.connAck()
                                .sessionPresent(sessionPresent)
                                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                                .build();
                        ctx.writeAndFlush(acceptAck);

                        // 连接成功相关处理
                        sysMessageService.onlineNotice(connMsg)
                                .doOnError(t -> log.error(t.getMessage(), t))
                                .subscribe();

                        // 会话有效性检查
                        if (!channel.isActive()) {
                            instanceRouterService.clearAdminClientConnect(session.getClientId()).subscribe();
                            log.warn("会话失效: 客户端ID=[{}], channel=[{}]", clientId, channel.id().asShortText());
                            return;
                        }

                        // 心跳超时设置
                        // [MQTT-3.1.2-24] If the Keep Alive value is non-zero and the Server does not receive a Control Packet from
                        // the Client within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection
                        // to the Client as if the network had failed
                        var heartbeat = variableHeader.keepAliveTimeSeconds() * 1.5;
                        if (heartbeat > 0) {
                            // 替换掉 NioChannelSocket 初始化时加入的 idleHandler
                            ctx.pipeline().replace(IdleStateHandler.class, "idleHandler", new IdleStateHandler(
                                    0, 0, (int) heartbeat));
                        }

                        // 根据协议补发 qos1,与 qos2 的消息(注意: 这里补发的是缓存中的消息，数据库中的消息由定时任务补发)
                        publishMessageService.search(clientId)
                                .publishOn(Schedulers.boundedElastic())
                                .doOnNext(pubMsg -> publishMessageService.republish(pubMsg).subscribe())
                                .subscribe();
                        log.info("连接成功: 客户端ID=[{}], channel=[{}]", clientId, channel.id());
                    })
                    .doOnError(t -> log.error(t.getMessage(), t))
                    .subscribe();
        }
    }


    /**
     * 生成一个唯一ID
     *
     * @return Unique Id
     */
    private String genClientId() {
        return NONE_ID_PREFIX + brokerId + "_" + System.currentTimeMillis();
    }

    /**
     * 当 conn cleanSession = 1,清理会话状态.会话状态由下列状态组成:
     * <ul>
     *     <li>The existence of a Session, even if the rest of the Session state is empty.</li>
     *     <li>The Client’s subscriptions.</li>
     *     <li>QoS 1 and QoS 2 messages which have been sent to the Client, but have not been completely acknowledged.</li>
     *     <li>QoS 1 and QoS 2 messages pending transmission to the Client.</li>
     *     <li>QoS 2 messages which have been received from the Client, but have not been completely acknowledged.</li>
     *     <li>Optionally, QoS 0 messages pending transmission to the Client.</li>
     * </ul>
     *
     * @param clientId 客户ID
     */
    private Mono<Void> actionOnCleanSession(String clientId) {
        // sessionService.clear(String ClientId) 方法返回 true 则表明该 clientId 之前的 cleanSession = false, 那么应该继续清理
        // 订阅信息、pub 信息、 pubRel 信息, 否则无需清理
        return sessionService.clear(clientId)
                .flatMap(e -> {
                    if (e) {
                        return Mono.when(
                                subscriptionService.clearClientSubscriptions(clientId, false),
                                publishMessageService.clear(clientId),
                                pubRelMessageService.clear(clientId));
                    } else {
                        return Mono.empty();
                    }
                });
    }

    /**
     * 获取客户端连接网口的IP:PORT信息
     *
     * @param ctx 见 {@link ChannelHandlerContext}
     * @return IP:PORT
     */
    private String clientAddress(ChannelHandlerContext ctx) {
        String proxyAddr = (String) ctx.channel().attr(AttributeKey.valueOf(ProxyAddrHandler.PROXY_ADDR)).get();
        if (StringUtils.hasText(proxyAddr)) {
            return proxyAddr;
        }

        SocketAddress remoteAddress = ctx.channel().remoteAddress();
        if (remoteAddress instanceof InetSocketAddress inetSocketAddress) {
            String host = inetSocketAddress.getHostString();
            int port = inetSocketAddress.getPort();
            return host + ":" + port;
        }

        return "unknown";
    }

    @Override
    public void action(byte[] msg) {
        InternalMessage<ClientConnOrDiscMsg> im;
        if (serializer instanceof JsonSerializer se) {
            im = se.deserialize(msg, new TypeReference<>() {
            });
        } else {
            //noinspection unchecked
            im = serializer.deserialize(msg, InternalMessage.class);
        }

        Optional.ofNullable(im.getData())
                .map(data -> {
                    String clientId = data.getClientId();
                    String instanceId = data.getInstanceId();
                    ALL_CLIENT_MAP.put(clientId, instanceId);
                    return clientId;
                })
                .map(ConnectHandler.CLIENT_MAP::remove)
                .map(BrokerHandler.CHANNELS::find)
                .map(ChannelOutboundInvoker::close);
    }

    @Override
    public boolean support(String channel) {
        return CONNECT.equals(channel);
    }
}