package com.hycan.idn.mqttx.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.consumer.Watcher;
import com.hycan.idn.mqttx.pojo.ClientConnOrDiscMsg;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.SysMsg;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.service.ISysMessageService;
import com.hycan.idn.mqttx.utils.JSON;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
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
public class SysMessageServiceImpl implements ISysMessageService, Watcher {

    /**
     * 上线事件超时时间 30 秒
     */
    private static final int SYS_EVENT_EXPIRE_MILLIS = 30 * 1000;

    private final ISubscriptionService subscriptionService;

    private final IInternalMessageService internalMessageService;

    private final Serializer serializer;

    private final String brokerId, SYS;
    private final boolean enableSysTopic, enableSysBridge, isClusterMode;

    private String kafkaBridgeTopic;
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public SysMessageServiceImpl(MqttxConfig config,
                                 @Nullable KafkaTemplate<String, byte[]> kafkaTemplate,
                                 ISubscriptionService subscriptionService,
                                 IInternalMessageService internalMessageService,
                                 Serializer serializer) {
        this.subscriptionService = subscriptionService;

        this.brokerId = config.getBrokerId();
        this.enableSysTopic = config.getSysTopic().getEnable();
        this.enableSysBridge = config.getMessageBridge().getEnableSys();
        this.internalMessageService = internalMessageService;
        this.serializer = serializer;
        if (Boolean.TRUE.equals(enableSysBridge)) {
            this.kafkaTemplate = kafkaTemplate;
            this.kafkaBridgeTopic = config.getMessageBridge().getKafkaSysBridgeTopic();

            Assert.notNull(kafkaBridgeTopic, "消息桥接Kafka主题不能为空!!!");
        }

        this.SYS = config.getKafka().getSys();
        this.isClusterMode = config.getCluster().getEnable();
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

        return subscriptionService.searchSysTopicClients(topic)
                .flatMap(clientSub -> publish(clientSub.getClientId(), topic, msg.getTimestamp(), false))
                .then();
    }

    /**
     * 发布系统消息
     *
     * @param sysClientId         订阅系统消息的客户端ID
     * @param sysTopic            订阅系统消息对应的topic
     * @param disconnectTimestamp 客户端下线时间戳
     */
    private Mono<Void> publish(String sysClientId, String sysTopic, Long disconnectTimestamp, boolean isClusterMsg) {
        Channel channel = Optional.ofNullable(CLIENT_MAP.get(sysClientId))
                .map(BrokerHandler.CHANNELS::find)
                .orElse(null);

        if (Objects.isNull(channel)) {
            if (isClusterMode && !isClusterMsg) {
                SysMsg msg = SysMsg.of(sysClientId, sysTopic, disconnectTimestamp);
                internalMessageService.publish(
                        new InternalMessage<>(msg, System.currentTimeMillis(), brokerId), SYS);
            }
            return Mono.empty();
        }

        // publish 对象组装
        var bytes = disconnectTimestamp.toString().getBytes(StandardCharsets.UTF_8);
        var currentTime = Unpooled.buffer(bytes.length).writeBytes(bytes);
        var mpm = MqttMessageBuilders.publish()
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(sysTopic)
                .payload(currentTime)
                .build();

        channel.writeAndFlush(mpm.retain());
        return Mono.empty();
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
        kafkaTemplate.send(kafkaBridgeTopic, JSON.writeValueAsBytes(msg));
    }

    @Override
    public void action(byte[] msg) {
        InternalMessage<SysMsg> im;
        if (serializer instanceof JsonSerializer se) {
            im = se.deserialize(msg, new TypeReference<>() {
            });
        } else {
            //noinspection unchecked
            im = serializer.deserialize(msg, InternalMessage.class);
        }

        SysMsg imMsg = im.getData();
        if (Objects.isNull(imMsg)) {
            return;
        }

        publish(imMsg.getClientId(), imMsg.getTopic(), imMsg.getTimestamp(), true).subscribe();
    }

    @Override
    public boolean support(String channel) {
        return SYS.equals(channel);
    }
}
