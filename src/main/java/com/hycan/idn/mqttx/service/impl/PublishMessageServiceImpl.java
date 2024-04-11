package com.hycan.idn.mqttx.service.impl;

import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.constants.MongoConstants;
import com.hycan.idn.mqttx.delay.exec.LocalMsgDelayQueueExecutor;
import com.hycan.idn.mqttx.delay.exec.OfflineMsgDelayQueueExecutor;
import com.hycan.idn.mqttx.delay.pojo.LocalMsgDelay;
import com.hycan.idn.mqttx.delay.pojo.OfflineMsgDelay;
import com.hycan.idn.mqttx.entity.MqttxOfflineMsg;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.IPubRelMessageService;
import com.hycan.idn.mqttx.service.IPublishMessageService;
import com.hycan.idn.mqttx.utils.BytesUtil;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * publish message store by mongodb
 *
 * @author Shadow
 */
@Slf4j
@Service
public class PublishMessageServiceImpl implements IPublishMessageService {

    private static final String PAYLOAD_LOG_TYPE_HEX = "hex";

    private final ReactiveMongoTemplate mongoTemplate;
    private final OfflineMsgDelayQueueExecutor offlineMsgDelayQueueExecutor;
    private final LocalMsgDelayQueueExecutor localMsgDelayQueueExecutor;
    private final IPubRelMessageService pubRelMessageService;

    private final String payloadLogType;
    private final boolean enableTopicSubPubSecure, enableLog;

    private final ConcurrentMap<String, ConcurrentHashMap<Integer, PubMsg>> outer = new ConcurrentHashMap<>(500);

    public PublishMessageServiceImpl(MqttxConfig mqttxConfig,
                                     ReactiveMongoTemplate mongoTemplate,
                                     @Lazy OfflineMsgDelayQueueExecutor offlineMsgDelayQueueExecutor,
                                     @Lazy LocalMsgDelayQueueExecutor localMsgDelayQueueExecutor,
                                     IPubRelMessageService pubRelMessageService) {
        this.mongoTemplate = mongoTemplate;
        this.offlineMsgDelayQueueExecutor = offlineMsgDelayQueueExecutor;
        this.localMsgDelayQueueExecutor = localMsgDelayQueueExecutor;
        this.pubRelMessageService = pubRelMessageService;

        this.enableTopicSubPubSecure = mqttxConfig.getFilterTopic().getEnableTopicSubPubSecure();
        this.enableLog = mqttxConfig.getSysConfig().getEnableLog();
        this.payloadLogType = mqttxConfig.getSysConfig().getPayloadLogType();

        Assert.notNull(mongoTemplate, "mongoTemplate can't be null");
    }

    @Override
    public Mono<Void> save2Db(String clientId, PubMsg pubMsg) {
        int messageId = pubMsg.getMessageId();
        outer.computeIfAbsent(clientId, s -> new ConcurrentHashMap<>(100)).putIfAbsent(messageId, PubMsg.of(pubMsg, clientId));
        offlineMsgDelayQueueExecutor.addQueue(OfflineMsgDelay.of(clientId, messageId));
        return Mono.empty();
    }

    @Override
    public void save2Cache(Session session, PubMsg pubMsg) {
        session.savePubMsg(pubMsg.getMessageId(), pubMsg);
        localMsgDelayQueueExecutor.addQueue(LocalMsgDelay.of(session, pubMsg.getMessageId()));
    }

    /**
     * 清除指定客户端的离线消息，考虑实际应用没有 cleanSession 动态变化的场景，此处仅清理数据库中的离线消息
     * 缓存中的离线消息处理太繁琐，直接不处理，等待缓存过期，过期时间默认是 25 秒
     *
     * @param clientId 客户端id
     */
    @Override
    public Mono<Void> clear(String clientId) {
        Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(clientId));
        return mongoTemplate.remove(query, MqttxOfflineMsg.class).then();
    }

    @Override
    public Mono<Void> remove(String clientId, int messageId) {
        // 1 从缓存中删除
        if (Objects.nonNull(removeCache(clientId, messageId))) {
            return Mono.empty();
        }

        // 2 从 MongoDB 中删除
        Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(clientId)
                .and(MongoConstants.MESSAGE_ID).is(messageId));
        return mongoTemplate.remove(query, MqttxOfflineMsg.class)
                .doOnError(t -> log.error("删除数据库离线消息异常, 异常详情=[{}]", t.getMessage()))
                .then();
    }

    @Override
    public PubMsg removeCache(String clientId, int messageId) {
        ConcurrentHashMap<Integer, PubMsg> inner = outer.get(clientId);
        if (CollectionUtils.isEmpty(inner)) {
            return null;
        }

        PubMsg pubMsg = inner.remove(messageId);
        if (CollectionUtils.isEmpty(outer.get(clientId))) {
            outer.remove(clientId);
        }
        return pubMsg;
    }

    /**
     * 存储离线消息的时候，没有对 clientId + messageId 执行 upsert 操作，这里查询的时候，按 messageId分组，返回日期最近的一条数据即可
     *
     * @param clientId 客户端id
     * @return
     */
    @Override
    public Flux<PubMsg> search(String clientId) {
        ConcurrentHashMap<Integer, PubMsg> inner = outer.get(clientId);
        List<PubMsg> pubMsgList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(inner)) {
            pubMsgList = inner.values().stream().toList();
        }

        return Flux.fromIterable(pubMsgList);
    }

    /**
     * 客户端(cleanSession = false)上线，补发 qos1,2 消息
     *
     * @param pubMsg {@link PubMsg}
     */
    @Override
    public Mono<Void> republish(PubMsg pubMsg) {
        final var clientId = pubMsg.getClientId();
        final var topic = pubMsg.getTopic();
        final var qos = pubMsg.getQos();
        final var messageId = pubMsg.getMessageId();
        final var payload = pubMsg.getPayload();

        final var channel = Optional.of(pubMsg.getClientId())
                .map(ConnectHandler.CLIENT_MAP::get)
                .map(BrokerHandler.CHANNELS::find)
                .orElse(null);
        if (Objects.isNull(channel)) {
            return Mono.empty();
        }

        return Mono.fromRunnable(() -> {

            // 订阅权限判定
            if (enableTopicSubPubSecure && !TopicUtils.hasAuthToSubTopic(channel, topic)) {
                return;
            }

            // It MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent to a Client because it matches an
            // established subscription regardless of how the flag was set in the message it received [MQTT-3.3.1-9].
            // The DUP flag MUST be set to 0 for all QoS 0 messages [MQTT-3.3.1-2].
            final var dupFlag = qos != MqttQoS.AT_MOST_ONCE.value();
            final var mqttMessage = MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.PUBLISH, dupFlag, MqttQoS.valueOf(qos),
                            false, 0),
                    new MqttPublishVariableHeader(topic, messageId),
                    // 这是一个浅拷贝，任何对pubMsg中payload的修改都会反馈到wrappedBuffer
                    Unpooled.wrappedBuffer(payload)
            );

            if (Boolean.TRUE.equals(enableLog)) {
                log.info("补发消息: 客户端ID=[{}], Topic=[{}], QoS=[{}], PacketId=[{}], Payload=[{}]",
                        clientId, topic, qos, messageId, PAYLOAD_LOG_TYPE_HEX.equals(payloadLogType) ?
                                BytesUtil.bytesToHexString(payload) : new String(payload, StandardCharsets.UTF_8));
            }
            channel.writeAndFlush(mqttMessage);
        })
        .thenMany(pubRelMessageService.searchOut(clientId))
        .doOnNext(msgId -> {
            var mqttMessage = MqttMessageFactory.newMessage(
                    // pubRel 的 fixHeader 是固定死了的 [0,1,1,0,0,0,1,0]
                    new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                    MqttMessageIdVariableHeader.from(msgId),
                    null
            );

            channel.writeAndFlush(mqttMessage);
        })
        .then();
    }
}
