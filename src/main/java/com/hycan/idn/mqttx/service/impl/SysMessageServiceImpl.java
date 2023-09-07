package com.hycan.idn.mqttx.service.impl;

import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.pojo.ClientConnOrDiscMsg;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.service.ISysMessageService;
import com.hycan.idn.mqttx.utils.Serializer;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

import static com.hycan.idn.mqttx.broker.handler.ConnectHandler.CLIENT_MAP;

/**
 * SYS事件相关业务
 *
 * @author shichongying
 * @datetime 2023年 03月 07日 16:37
 */
@Slf4j
@Service
public class SysMessageServiceImpl implements ISysMessageService {

    /** 上线事件超时时间 30 秒 */
    private static final int SYS_EVENT_EXPIRE_MILLIS = 30 * 1000;

    private final ReactiveStringRedisTemplate redisTemplate;

    private final ISubscriptionService subscriptionService;

    private final Serializer serializer;

    private final String  clientSysMsgPrefix, brokerId;
    private final boolean enableSysTopic, enableSysBridge;

    private String kafkaBridgeTopic;
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public SysMessageServiceImpl(MqttxConfig config,
                                 ReactiveStringRedisTemplate redisTemplate,
                                 @Nullable KafkaTemplate<String, byte[]> kafkaTemplate,
                                 ISubscriptionService subscriptionService,
                                 Serializer serializer) {
        this.redisTemplate = redisTemplate;
        this.subscriptionService = subscriptionService;

        this.brokerId = config.getBrokerId();
        this.enableSysTopic = config.getSysTopic().getEnable();
        this.enableSysBridge = config.getMessageBridge().getEnableSys();
        if (Boolean.TRUE.equals(enableSysBridge)) {
            this.kafkaTemplate = kafkaTemplate;
            this.kafkaBridgeTopic = config.getMessageBridge().getKafkaSysBridgeTopic();

            Assert.notNull(kafkaBridgeTopic, "消息桥接Kafka主题不能为空!!!");
        }

        this.clientSysMsgPrefix = config.getRedis().getClientSysMsgPrefix();
        this.serializer = serializer;
    }

    /**
     * 上线消息提醒
     */
    @Override
    public Mono<Void> onlineNotice(ClientConnOrDiscMsg msg) {
        final String topic = String.format(TopicUtils.BROKER_CLIENT_CONNECT, brokerId, msg.getClientId());
        return sysNotice(msg, topic).then();
    }

    /**
     * 下线消息提醒
     */
    @Override
    public Mono<Void> offlineNotice(ClientConnOrDiscMsg msg) {
        final String topic = String.format(TopicUtils.BROKER_CLIENT_DISCONNECT, brokerId, msg.getClientId());
        return sysNotice(msg, topic).then();
    }

    /**
     * 上/下线提醒
     */
    private Mono<Void> sysNotice(ClientConnOrDiscMsg msg, String topic) {
        if (!enableSysTopic) {
            return Mono.empty();
        }

        if ((System.currentTimeMillis() - msg.getTimestamp()) >= SYS_EVENT_EXPIRE_MILLIS) {
            return Mono.empty();
        }

        // publish 对象组装
        var bytes = LocalDateTime.now().toString().getBytes(StandardCharsets.UTF_8);
        var currentTime = Unpooled.buffer(bytes.length).writeBytes(bytes);
        var mpm = MqttMessageBuilders.publish()
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(topic)
                .payload(currentTime)
                .build();

        return subscriptionService.searchSysTopicClients(topic)
                .flatMap(clientSub -> {
                    Optional<Channel> channelOptional = Optional.ofNullable(CLIENT_MAP.get(clientSub.getClientId()))
                            .map(BrokerHandler.CHANNELS::find);
                    if (channelOptional.isPresent()) {
                        return publish(msg, mpm, clientSub.getTopic(), channelOptional.get());
                    }
                    return Mono.empty();
                })
                .doOnComplete(mpm::release)
                .then();
    }

    /**
     * 发布系统消息
     *
     * @param msg       客户端上/下线集群消息
     * @param mpm       组装完整的消息
     * @param topic     订阅系统事件的客户端 Topic
     * @param channel   订阅系统事件客户端对应的 channel
     */
    private Mono<Void> publish(ClientConnOrDiscMsg msg, MqttPublishMessage mpm, String topic, Channel channel) {
        if (!TopicUtils.isShare(topic)) {
            channel.writeAndFlush(mpm.retain());
            return Mono.empty();
        }

        // 共享订阅上下线主题的客户端，只能有其中一个客户端接收上下线消息，这里使用 setnx 加锁处理，集群内 mqttx 节点竞争到锁则发消息
        return redisTemplate.opsForValue().setIfAbsent(key(msg), msg.getType(), Duration.ofSeconds(30))
                .doOnSuccess(isSuccess -> {
                    if (Boolean.TRUE.equals(isSuccess)) {
                        channel.writeAndFlush(mpm.retain());
                    }
                })
                .then();
    }

    /**
     * 发送桥接的客户端上下线消息
     *
     * @param msg 客户端上/下线集群消息
     */
    @Override
    public void publishSysBridge(ClientConnOrDiscMsg msg) {
        if (!enableSysBridge) {
            return;
        }
        kafkaTemplate.send(kafkaBridgeTopic, serializer.serialize(msg));
    }

    private String key(ClientConnOrDiscMsg msg) {
        return clientSysMsgPrefix + msg.getClientId() + "_" + msg.getType() + "_" + msg.getTimestamp();
    }
}
