package com.hycan.idn.mqttx.graceful;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.consumer.Watcher;
import com.hycan.idn.mqttx.graceful.event.MqttxStateEvent;
import com.hycan.idn.mqttx.pojo.ClientSub;
import com.hycan.idn.mqttx.pojo.ConnectInfo;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.SyncMsg;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.hycan.idn.mqttx.graceful.InstanceChangeListener.ACTIVE_INSTANCE_SET;

/**
 * 当前节点实例状态变化监听器
 *
 * @author Shadow
 * @datetime 2023-11-10 11:03
 */
@Slf4j
@Component
public class CurrentMqttxStateListener implements Watcher {

    public static final String IS_INSTANCE_DOWN_EVENT = "isInstanceDownEvent";

    @Resource
    private KafkaListenerEndpointRegistry endpointRegistry;

    private final MqttxHealthIndicator mqttxHealthIndicator;

    private final ISubscriptionService subscriptionService;

    private final Serializer serializer;

    private final String currentInstanceId, SYNC;
    private final int gracefulStartTime;

    public CurrentMqttxStateListener(MqttxHealthIndicator mqttxHealthIndicator,
                                     ISubscriptionService subscriptionService,
                                     Serializer serializer, MqttxConfig mqttxConfig) {
        this.mqttxHealthIndicator = mqttxHealthIndicator;
        this.subscriptionService = subscriptionService;
        this.serializer = serializer;
        this.currentInstanceId = mqttxConfig.getInstanceId();
        this.SYNC = mqttxConfig.getKafka().getSync();
        this.gracefulStartTime = mqttxConfig.getSysConfig().getGracefulStartTime();
    }

    @EventListener(condition = "T(com.hycan.idn.mqttx.graceful.event.MqttxStateEvent).INSTANCE_STARTUP.equals(#event.state)")
    public void instanceStartup(MqttxStateEvent event) {
        List<String> onlineInstanceList = event.getInstanceIds();
        // 当前节点的活跃实例数为空时，说明该服务当前非上线状态
        if (!ACTIVE_INSTANCE_SET.isEmpty()) {
            return;
        }

        // 实例未启动完成时，也会收到nacos推送实例变化事件，不做处理
        if (!onlineInstanceList.contains(currentInstanceId)) {
            return;
        }

        ACTIVE_INSTANCE_SET.addAll(onlineInstanceList);
        log.info("MQTTX实例启动中: 活跃实例列表={}", ACTIVE_INSTANCE_SET);

        if (!endpointRegistry.isRunning()) {
            endpointRegistry.start();
            log.info("MQTTX实例启动中: 启动Kafka消费者, 运行状态=[{}]", endpointRegistry.isRunning() ? "运行中" : "已停止");
        }

        try {
            // 等待 30 秒后，将服务 readness 健康检查状态设置上线，预留充足时间同步连接/订阅数据
            TimeUnit.SECONDS.sleep(gracefulStartTime);
            mqttxHealthIndicator.setHealth(true);
            log.info("MQTTX实例启动完成: 设置健康检查状态=[true]");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @EventListener(condition = "T(com.hycan.idn.mqttx.graceful.event.MqttxStateEvent).INSTANCE_SHUTDOWN.equals(#event.state)")
    public void instanceShutdown(MqttxStateEvent event) {
        List<String> offlineInstanceList = event.getInstanceIds();
        if (!offlineInstanceList.contains(currentInstanceId)) {
            return;
        }

        ACTIVE_INSTANCE_SET.clear();
        log.info("MQTTX实例下线中: 活跃实例列表={}", ACTIVE_INSTANCE_SET);

        // 停止Kafka
        if (endpointRegistry.isRunning()) {
            endpointRegistry.stop();
            log.info("MQTTX实例下线: 停止Kafka消费者, 运行状态=[{}]", endpointRegistry.isRunning() ? "运行中" : "已停止");
        }

        // 正在下线的节点收到服务停止通知，踢掉自己维护的所有连接
        for (ChannelId channelId : ConnectHandler.CLIENT_MAP.values()) {
            Optional.ofNullable(channelId)
                    .map(BrokerHandler.CHANNELS::find)
                    .map(channel -> {
                        channel.attr(AttributeKey.valueOf(IS_INSTANCE_DOWN_EVENT)).set(true);
                        return channel;
                    })
                    .ifPresent(ChannelOutboundInvoker::close);
        }
    }

    @Override
    public void action(byte[] msg) {
        InternalMessage<SyncMsg> im;
        if (serializer instanceof JsonSerializer se) {
            im = se.deserialize(msg, new TypeReference<>() {
            });
        } else {
            //noinspection unchecked
            im = serializer.deserialize(msg, InternalMessage.class);
        }

        SyncMsg syncMsg = im.getData();
        String instanceId = syncMsg.getInstanceId();
        if (!StringUtils.hasText(instanceId)) {
            return;
        }

        List<String> clientIds = syncMsg.getClientIds();
        if (!CollectionUtils.isEmpty(clientIds)) {
            log.info("接收其他MQTTX实例客户端连接数据: 实例ID=[{}], 数量=[{}]", currentInstanceId, clientIds.size());
            clientIds.forEach(clientId ->
                    ConnectHandler.ALL_CLIENT_MAP.put(clientId, ConnectInfo.of(instanceId, System.currentTimeMillis())));
        }

        List<ClientSub> clientSubs = syncMsg.getClientSubs();
        if (!CollectionUtils.isEmpty(clientSubs)) {
            log.info("接收其他MQTTX实例客户端订阅数据: 实例ID=[{}], 数量=[{}]", currentInstanceId, clientSubs.size());
            clientSubs.forEach(clientSub -> {
                // cleanSession == false 的订阅信息，服务启动时会从数据库加载
                if (!clientSub.isCleanSession()) {
                    return;
                }

                boolean isSysTopic = TopicUtils.isSys(clientSub.getTopic());
                if (isSysTopic) {
                    subscriptionService.subscribeSys(clientSub, false).subscribe();
                } else {
                    subscriptionService.subscribeWithCache(clientSub);
                }
            });
        }

    }

    @Override
    public boolean support(String channel) {
        return SYNC.equals(channel);
    }
}
