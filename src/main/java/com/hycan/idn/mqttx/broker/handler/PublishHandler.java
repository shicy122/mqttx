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
import com.hycan.idn.mqttx.exception.AuthorizationException;
import com.hycan.idn.mqttx.pojo.BridgeMsg;
import com.hycan.idn.mqttx.pojo.ClientSub;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.service.IPubRelMessageService;
import com.hycan.idn.mqttx.service.IPublishMessageService;
import com.hycan.idn.mqttx.service.IRetainMessageService;
import com.hycan.idn.mqttx.service.ISessionService;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.utils.BoundedExecutor;
import com.hycan.idn.mqttx.utils.BytesUtil;
import com.hycan.idn.mqttx.utils.JSON;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.RateLimiter;
import com.hycan.idn.mqttx.utils.Serializer;
import com.hycan.idn.mqttx.utils.ThreadPoolUtil;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * {@link MqttMessageType#PUBLISH} 发布消息 处理器
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Handler(type = MqttMessageType.PUBLISH)
public class PublishHandler extends AbstractMqttTopicSecureHandler implements Watcher {
    //@formatter:off

    private static final String PAYLOAD_LOG_TYPE_HEX = "hex";

    private final ISessionService sessionService;
    private final IRetainMessageService retainMessageService;
    private final ISubscriptionService subscriptionService;
    private final IPublishMessageService publishMessageService;
    private final IPubRelMessageService pubRelMessageService;
    private final IInternalMessageService internalMessageService;
    private final String brokerId, PUBLISH, payloadLogType;
    private final boolean enableTopicSubPubSecure, enableShareTopic, enableRateLimiter,
            ignoreClientSelfPub, enableBizBridge, enableLog, enablePayloadLog;
    /**
     * 主题限流器
     */
    private final Map<String, RateLimiter> rateLimiterMap = new HashMap<>();
    private final Serializer serializer;

    private Pattern MQTT_TOPIC_PATTERN;
    private String kafkaBridgeTopic;
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    private final BoundedExecutor boundedExecutor;

    //@formatter:on

    public PublishHandler(IPublishMessageService publishMessageService,
                          IRetainMessageService retainMessageService,
                          ISubscriptionService subscriptionService,
                          IPubRelMessageService pubRelMessageService,
                          ISessionService sessionService,
                          @Nullable IInternalMessageService internalMessageService,
                          MqttxConfig config,
                          @Nullable KafkaTemplate<String, byte[]> kafkaTemplate,
                          Serializer serializer) {
        super(config.getCluster().getEnable());
        Assert.notNull(publishMessageService, "publishMessageService can't be null");
        Assert.notNull(retainMessageService, "retainMessageService can't be null");
        Assert.notNull(subscriptionService, "publishMessageService can't be null");
        Assert.notNull(pubRelMessageService, "publishMessageService can't be null");
        Assert.notNull(config, "mqttxConfig can't be null");

        var shareTopic = config.getShareTopic();
        var messageBridge = config.getMessageBridge();
        var rateLimiter = config.getRateLimiter();
        this.sessionService = sessionService;
        this.serializer = serializer;
        this.publishMessageService = publishMessageService;
        this.retainMessageService = retainMessageService;
        this.subscriptionService = subscriptionService;
        this.pubRelMessageService = pubRelMessageService;
        this.brokerId = config.getBrokerId();
        this.PUBLISH = config.getKafka().getPub();
        this.enableTopicSubPubSecure = config.getFilterTopic().getEnableTopicSubPubSecure();
        this.ignoreClientSelfPub = config.getIgnoreClientSelfPub();
        this.enableShareTopic = shareTopic.getEnable();
        if (!CollectionUtils.isEmpty(rateLimiter.getTopicRateLimits()) && Boolean.TRUE.equals(rateLimiter.getEnable())) {
            enableRateLimiter = true;
            rateLimiter.getTopicRateLimits()
                    .forEach(topicRateLimit -> rateLimiterMap.put(
                            topicRateLimit.getTopic(),
                            new RateLimiter(topicRateLimit.getCapacity(),
                                    topicRateLimit.getReplenishRate(),
                                    topicRateLimit.getTokenConsumedPerAcquire())));
        } else {
            enableRateLimiter = false;
        }

        this.enableBizBridge = messageBridge.getEnableBiz();
        if (Boolean.TRUE.equals(enableBizBridge)) {
            this.kafkaTemplate = kafkaTemplate;
            this.kafkaBridgeTopic = messageBridge.getKafkaBizBridgeTopic();
            this.MQTT_TOPIC_PATTERN = Pattern.compile(messageBridge.getMqttBizTopicPattern());

            Assert.notNull(MQTT_TOPIC_PATTERN, "消息桥接正则表达式不能为空!!!");
            Assert.notNull(kafkaBridgeTopic, "消息桥接Kafka主题不能为空!!!");
        }

        this.internalMessageService = internalMessageService;
        Assert.notNull(internalMessageService, "internalMessageService can't be null");

        this.enableLog = config.getSysConfig().getEnableLog();
        this.enablePayloadLog = config.getSysConfig().getEnablePayloadLog();
        this.payloadLogType = config.getSysConfig().getPayloadLogType();

        ExecutorService executorService = ThreadPoolUtil.newThreadPoolExecutor(
                4, ThreadPoolUtil.THREAD_NAME_SEND_BRIDGE_MSG);
        boundedExecutor = new BoundedExecutor(executorService, ThreadPoolUtil.DEFAULT_CAPACITY);
    }

    /**
     * 根据 MQTT v3.1.1 Qos2 实现有 Method A 与 Method B,这里采用 B 方案，
     * 具体参见 <b>Figure 4.3-Qos protocol flow diagram,non normative example</b>
     *
     * @param ctx 见 {@link ChannelHandlerContext}
     * @param msg 解包后的数据
     */
    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {
        final var mpm = (MqttPublishMessage) msg;
        final var mqttFixedHeader = mpm.fixedHeader();
        final var mqttPublishVariableHeader = mpm.variableHeader();
        final var payload = mpm.payload();

        // 获取qos、topic、packetId、retain、payload
        final var qos = mqttFixedHeader.qosLevel();
        final var topic = mqttPublishVariableHeader.topicName();
        final var packetId = mqttPublishVariableHeader.packetId();
        final var retain = mqttFixedHeader.isRetain();
        final var data = new byte[payload.readableBytes()];
        payload.readBytes(data);

        if (Boolean.TRUE.equals(enableLog)) {
            if (enablePayloadLog) {
                log.info("接收消息: 客户端ID=[{}], Topic=[{}], QoS=[{}], PacketId=[{}], Payload=[{}]",
                        clientId(ctx), topic, qos, packetId, PAYLOAD_LOG_TYPE_HEX.equals(payloadLogType) ?
                                BytesUtil.bytesToHexString(data) : new String(data, StandardCharsets.UTF_8));
            } else {
                log.info("接收消息: 客户端ID=[{}], Topic=[{}], QoS=[{}], PacketId=[{}]", clientId(ctx), topic, qos, packetId);
            }
        }

        // 发布权限判定
        if (enableTopicSubPubSecure && !hasAuthToPubTopic(ctx, topic)) {
            throw new AuthorizationException("无对应 topic=[" + topic + "] 发布权限");
        }

        // 消息桥接 kafka
        if (Boolean.TRUE.equals(enableBizBridge)) {
            boundedExecutor.submitTask(() -> {
                if (MQTT_TOPIC_PATTERN.matcher(topic).matches()) {
                    kafkaTemplate.send(kafkaBridgeTopic, JSON.writeValueAsBytes(BridgeMsg.of(topic, data)));
                }
            });
        }
        // 限流判定, 满足如下四个条件即被限流：
        // 1 限流器开启
        // 2 qos = 0
        // 3 该主题配置了限流器
        // 4 令牌获取失败
        // 被限流的消息就会被直接丢弃
        if (enableRateLimiter &&
                qos == MqttQoS.AT_MOST_ONCE &&
                rateLimiterMap.containsKey(topic) &&
                !rateLimiterMap.get(topic).acquire(Instant.now().getEpochSecond())) {
            return;
        }

        // 组装消息
        // When sending a PUBLISH Packet to a Client the Server MUST set the RETAIN flag to 1 if a message is sent as a
        // result of a new subscription being made by a Client [MQTT-3.3.1-8]. It MUST set the RETAIN flag to 0 when a
        // PUBLISH Packet is sent to a Client because it matches an established subscription regardless of how the flag
        // was set in the message it received [MQTT-3.3.1-9].
        // 当新 topic 订阅触发 retain 消息时，retain flag 才应该置 1，其它状况都是 0.
        final var pubMsg = PubMsg.of(qos.value(), topic, false, data);
        Session session = getSession(ctx);

        // 响应
        switch (qos) {
            case AT_MOST_ONCE -> publish(pubMsg, session, false)
                    .publishOn(Schedulers.boundedElastic())
                    .doOnSuccess(unused -> {
                        if (retain) {
                            handleRetainMsg(pubMsg).subscribe();
                        }
                    }).subscribe();
            case AT_LEAST_ONCE -> publish(pubMsg, session, false)
                    .publishOn(Schedulers.boundedElastic())
                    .doOnSuccess(unused -> {
                        MqttMessage pubAck = MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                MqttMessageIdVariableHeader.from(packetId),
                                null
                        );
                        ctx.writeAndFlush(pubAck);

                        // retain 消息处理
                        if (retain) {
                            handleRetainMsg(pubMsg).subscribe();
                        }
                    }).subscribe();
            case EXACTLY_ONCE -> {
                // 判断消息是否重复, 未重复的消息需要保存 messageId
                if (Boolean.TRUE.equals(session.getCleanSession())) {
                    if (!session.isDupMsg(packetId)) {
                        publish(pubMsg, session, false)
                                .publishOn(Schedulers.boundedElastic())
                                .doOnSuccess(unused -> {
                                    // 保存 pub
                                    session.savePubRelInMsg(packetId);

                                    // 组装PUBREC消息
                                    var pubRec = MqttMessageFactory.newMessage(
                                            new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE,
                                                    false, 0),
                                            MqttMessageIdVariableHeader.from(packetId),
                                            null
                                    );
                                    ctx.writeAndFlush(pubRec);

                                    // retain 消息处理
                                    if (retain) {
                                        handleRetainMsg(pubMsg).subscribe();
                                    }
                                }).subscribe();
                    } else {
                        var pubRec = MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                MqttMessageIdVariableHeader.from(packetId),
                                null
                        );
                        ctx.writeAndFlush(pubRec);

                        // retain 消息处理
                        if (retain) {
                            handleRetainMsg(pubMsg).subscribe();
                        }
                    }
                } else {
                    pubRelMessageService.isInMsgDup(session.getClientId(), packetId)
                            .flatMap(isInMsgDup -> {
                                if (Boolean.TRUE.equals(isInMsgDup)) {
                                    return Mono.empty();
                                } else {
                                    return publish(pubMsg, session, false)
                                            .publishOn(Schedulers.boundedElastic())
                                            .doOnSuccess(unused -> pubRelMessageService.saveIn(session.getClientId(), packetId)
                                                    .subscribe());
                                }
                            })
                            .publishOn(Schedulers.boundedElastic())
                            .doOnSuccess(unused -> {
                                var pubRec = MqttMessageFactory.newMessage(
                                        new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false,
                                                0),
                                        MqttMessageIdVariableHeader.from(packetId),
                                        null
                                );
                                ctx.writeAndFlush(pubRec);

                                // retain 消息处理
                                if (retain) {
                                    handleRetainMsg(pubMsg).subscribe();
                                }
                            })
                            .subscribe();
                }
            }
        }
    }

    /**
     * 消息发布，目前看来 {@link PubMsg} 的来源有如下几种：
     * <ol>
     *     <li>{@link MqttMessageType#PUBLISH} 消息</li>
     *     <li>遗嘱消息</li>
     *     <li>retain 消息被新订阅触发 </li>
     *     <li>集群消息 {@link #action(byte[])}</li>
     * </ol>
     *
     * @param pubMsg           publish message
     * @param session              {@link Session} MQTT 会话
     * @param isClusterMessage 标志消息源是集群还是客户端
     */
    public Mono<Void> publish(final PubMsg pubMsg, Session session, boolean isClusterMessage) {
        // 指定了客户端的消息
        if (StringUtils.hasText(pubMsg.getClientId())) {
            final var clientId = pubMsg.getClientId();
            return isCleanSession(clientId)
                    .flatMap(isCleanSession ->
                            publish0(ClientSub.of(clientId, pubMsg.getQos(), pubMsg.getTopic(), isCleanSession),
                                    pubMsg, isClusterMessage))
                    .then();
        }

        Flux<ClientSub> clientSubFlux = subscriptionService.searchSubscribeClientList(pubMsg.getTopic())
                .filter(clientSub -> {
                    if (ignoreClientSelfPub) {
                        // 忽略 client 自身的订阅
                        return !Objects.equals(clientSub.getClientId(), session.getClientId());
                    }
                    return true;
                });

        return clientSubFlux
                .collectList()
                .flatMapIterable(Function.identity())
                .flatMap(clientSub -> {
                    // 未开启共享订阅，但是 client 订阅了含 $share 的 topic
                    if (!enableShareTopic && TopicUtils.isShare(clientSub.getTopic())) {
                        return Mono.empty();
                    }

                    // 将消息推送给集群中的 broker
                    if (isClusterMode() && !isClusterMessage) {
                        String clientId = clientSub.getClientId();
                        // 连接不在当前 mqttx && 连接在其它mqttx, 则发送消息给集群
                        if (!ConnectHandler.CLIENT_MAP.containsKey(clientId)
                                && ConnectHandler.ALL_CLIENT_MAP.containsKey(clientId)) {
                            // 指定客户端发送，收到集群消息的mqttx处理流程更简单
                            pubMsg.setClientId(clientId);
                            return internalMessagePublish(pubMsg);
                        }
                    }

                    return publish0(clientSub, pubMsg, isClusterMessage);
                })
                .then();
    }

    /**
     * 发布消息给 clientSub
     *
     * @param clientSub        {@link ClientSub}
     * @param pubMsg           待发布消息
     */
    private Mono<Void> publish0(ClientSub clientSub, PubMsg pubMsg, boolean isClusterMessage) {
        // clientId, channel, topic
        final String clientId = clientSub.getClientId();
        final boolean isCleanSession = clientSub.isCleanSession();
        final var channel = Optional.of(clientId)
                .map(ConnectHandler.CLIENT_MAP::get)
                .map(BrokerHandler.CHANNELS::find)
                .orElse(null);
        final var topic = pubMsg.getTopic();

        // 计算Qos
        final var pubQos = pubMsg.getQos();
        final var subQos = clientSub.getQos();
        final var qos = subQos >= pubQos ? MqttQoS.valueOf(pubQos) : MqttQoS.valueOf(subQos);

        // payload, retained flag
        final var payload = pubMsg.getPayload();
        final var retained = pubMsg.isRetain();

        // 接下来的处理分四种情况
        // 1. channel == null && cleanSession  => 直接返回，由集群中其它的 broker 处理（pubMsg 无 messageId）
        // 2. channel == null && !cleanSession => 保存 pubMsg （pubMsg 有 messageId）
        // 3. channel != null && cleanSession  => 将消息关联到会话，并发送 publish message 给 client（messageId 取自 session）
        // 4. channel != null && !cleanSession => 将消息持久化到 redis, 并发送 publish message 给 client（messageId redis increment 指令）

        // 1. channel == null && cleanSession
        if (channel == null && isCleanSession) {
            log.warn("未发消息: 客户端ID=[{}]对应的channel不存在, cleanSession=[true], Topic=[{}], QoS=[{}], Payload=[{}], 连接当前节点=[{}], 缓存连接关系=[{}]",
                    clientId, topic, qos, PAYLOAD_LOG_TYPE_HEX.equals(payloadLogType) ?
                            BytesUtil.bytesToHexString(payload) : new String(payload, StandardCharsets.UTF_8),
                    ConnectHandler.CLIENT_MAP.containsKey(clientId),
                    ConnectHandler.ALL_CLIENT_MAP.containsKey(clientId));

            return Mono.empty();
        }

        // 2. channel == null && !cleanSession
        if (channel == null) {
            log.warn("未发消息: 客户端ID=[{}]对应的channel不存在, cleanSession=[false], Topic=[{}], QoS=[{}], Payload=[{}], 连接当前节点=[{}], 缓存连接关系=[{}]",
                    clientId, topic, qos, PAYLOAD_LOG_TYPE_HEX.equals(payloadLogType) ?
                            BytesUtil.bytesToHexString(payload) : new String(payload, StandardCharsets.UTF_8),
                    ConnectHandler.CLIENT_MAP.containsKey(clientId),
                    ConnectHandler.ALL_CLIENT_MAP.containsKey(clientId));

            if ((qos == MqttQoS.EXACTLY_ONCE || qos == MqttQoS.AT_LEAST_ONCE) && !isClusterMessage) {
                return sessionService.nextMessageId(clientId)
                        .flatMap(messageId -> {
                            pubMsg.setQos(qos.value());
                            pubMsg.setMessageId(messageId);
                            return publishMessageService.save2Db(clientId, pubMsg);
                        });
            }
            return Mono.empty();
        }

        // 处理 channel != null 的情况
        // 计算 messageId
        int messageId;

        // 3. channel != null && cleanSession
        if (isCleanSession) {
            // cleanSession 状态下不判断消息是否为集群
            // 假设消息由集群内其它 broker 分发，而 cleanSession 状态下 broker 消息走的内存，为了实现 qos1,2 我们必须将消息保存到内存
            if ((qos == MqttQoS.EXACTLY_ONCE || qos == MqttQoS.AT_LEAST_ONCE)) {
                messageId = nextMessageId(channel);
                pubMsg.setMessageId(messageId);
                publishMessageService.save2Cache(getSession(channel), pubMsg);
            } else {
                // qos0
                messageId = 0;
            }
        } else {
            // 4. channel != null && !cleanSession
            if (qos == MqttQoS.EXACTLY_ONCE || qos == MqttQoS.AT_LEAST_ONCE) {
                return sessionService.nextMessageId(clientId)
                        .flatMap(nextMessageId -> {
                            pubMsg.setQos(qos.value());
                            pubMsg.setMessageId(nextMessageId);
                            return publishMessageService.save2Db(clientId, pubMsg)
                                    .doOnSuccess(unused -> {
                                        var mpm = new MqttPublishMessage(
                                                new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, retained, 0),
                                                new MqttPublishVariableHeader(topic, nextMessageId),
                                                Unpooled.wrappedBuffer(payload));

                                        if (Boolean.TRUE.equals(enableLog)) {
                                            if (enablePayloadLog) {
                                                log.info("发送消息: 客户端ID=[{}], Topic=[{}], QoS=[{}], PacketId=[{}], Payload=[{}]",
                                                        clientId, topic, qos, nextMessageId, PAYLOAD_LOG_TYPE_HEX.equals(payloadLogType) ?
                                                                BytesUtil.bytesToHexString(payload) : new String(payload, StandardCharsets.UTF_8));
                                            } else {
                                                log.info("发送消息: 客户端ID=[{}], Topic=[{}], QoS=[{}], PacketId=[{}]", clientId, topic, qos, nextMessageId);
                                            }
                                        }
                                        channel.writeAndFlush(mpm);
                            });
                        })
                        .then();
            } else {
                // qos0
                messageId = 0;
            }
        }

        // 发送报文给 client
        // mqttx 只有 ConnectHandler#republish(ChannelHandlerContext) 方法有必要将 dup flag 设置为 true(qos > 0), 其它应该为 false.
        var mpm = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, retained, 0),
                new MqttPublishVariableHeader(topic, messageId),
                Unpooled.wrappedBuffer(payload)
        );

        if (Boolean.TRUE.equals(enableLog)) {
            if (enablePayloadLog) {
                log.info("发送消息: 客户端ID=[{}], Topic=[{}], QoS=[{}], PacketId=[{}], Payload=[{}]",
                        clientId, topic, qos, messageId, PAYLOAD_LOG_TYPE_HEX.equals(payloadLogType) ?
                                BytesUtil.bytesToHexString(payload) : new String(payload, StandardCharsets.UTF_8));
            } else {
                log.info("发送消息: 客户端ID=[{}], Topic=[{}], QoS=[{}], PacketId=[{}]", clientId, topic, qos, messageId);
            }
        }
        channel.writeAndFlush(mpm);
        return Mono.empty();
    }

    /**
     * 处理 retain 消息
     *
     * @param pubMsg retain message
     */
    public Mono<Void> handleRetainMsg(PubMsg pubMsg) {
        // with issue https://github.com/Amazingwujun/mqttx/issues/14 And PR
        // https://github.com/Amazingwujun/mqttx/pull/15
        // If the Server receives a QoS 0 message with the RETAIN flag set to 1 it
        // MUST discard any message previously retained for that topic. It SHOULD
        // store the new QoS 0 message as the new retained message for that topic,
        // but MAY choose to discard it at any time - if this happens there will be
        // no retained message for that topic [MQTT-3.3.1-7].
        // [MQTT-3.3.1-7] 当 broker 收到 qos 为 0 并且 RETAIN = 1 的消息 必须抛弃该主题保留
        // 之前的消息（注意：是 retained 消息）, 同时 broker 可以选择保留或抛弃当前的消息，MQTTX
        // 的选择是保留.

        // A PUBLISH Packet with a RETAIN flag set to 1 and a payload containing zero
        // bytes will be processed as normal by the Server and sent to Clients with a
        // subscription matching the topic name. Additionally any existing retained
        // message with the same topic name MUST be removed and any future subscribers
        // for the topic will not receive a retained message [MQTT-3.3.1-10].
        // [MQTT-3.3.1-10] 注意 [Additionally] 内容， broker 收到 retain 消息载荷（payload）
        // 为空时，broker 必须移除 topic 关联的 retained 消息.

        byte[] payload = pubMsg.getPayload();
        if (ObjectUtils.isEmpty(payload)) {
            String topic = pubMsg.getTopic();
            return retainMessageService.remove(topic);
        }

        return retainMessageService.save(pubMsg);
    }

    /**
     * 集群内部消息发布
     *
     * @param pubMsg {@link PubMsg}
     */
    private Mono<Void> internalMessagePublish(PubMsg pubMsg) {
        var im = new InternalMessage<>(pubMsg, System.currentTimeMillis(), brokerId);
        internalMessageService.publish(im, PUBLISH);

        return Mono.empty();
    }

    @Override
    public void action(byte[] msg) {
        InternalMessage<PubMsg> im;
        if (serializer instanceof JsonSerializer s) {
            im = s.deserialize(msg, new TypeReference<>() {});
        } else {
            //noinspection unchecked
            im = serializer.deserialize(msg, InternalMessage.class);
        }
        PubMsg data = im.getData();
        publish(data, null, true).subscribe();
    }

    @Override
    public boolean support(String channel) {
        return PUBLISH.equals(channel);
    }

    /**
     * 判断 clientId 关联的会话是否是 cleanSession 会话
     *
     * @param clientId 客户端id
     * @return true if session is cleanSession
     */
    private Mono<Boolean> isCleanSession(String clientId) {
        return sessionService.hasKey(clientId).map(e -> !e);
    }
}
