package com.hycan.idn.mqttx.graceful;

import com.google.common.collect.Lists;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.graceful.event.MqttxStateEvent;
import com.hycan.idn.mqttx.pojo.ClientSub;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.SyncMsg;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hycan.idn.mqttx.graceful.InstanceChangeListener.ACTIVE_INSTANCE_SET;

/**
 * 集群节点实例状态变化监听器
 *
 * @author Shadow
 * @datetime 2023-11-10 11:40
 */
@Slf4j
@Component
public class InternalMqttxStateListener {

    private static final int SYNC_BATCH_SIZE_CLIENT_ID = 200;
    private static final int SYNC_BATCH_SIZE_CLIENT_SUB = 50;

    private final IInternalMessageService internalMessageService;

    private final ISubscriptionService subscriptionService;

    private final String brokerId, currentInstanceId, SYNC;
    private final boolean enableSysTopic;

    public InternalMqttxStateListener(IInternalMessageService internalMessageService,
                                      ISubscriptionService subscriptionService,
                                      MqttxConfig mqttxConfig) {
        this.internalMessageService = internalMessageService;
        this.subscriptionService = subscriptionService;
        this.brokerId = mqttxConfig.getBrokerId();
        this.currentInstanceId = mqttxConfig.getInstanceId();
        this.SYNC = mqttxConfig.getKafka().getSync();

        this.enableSysTopic = mqttxConfig.getSysTopic().getEnable();
    }

    @EventListener(condition = "T(com.hycan.idn.mqttx.graceful.event.MqttxStateEvent).INSTANCE_STARTUP.equals(#event.state)")
    public void instanceOnline(MqttxStateEvent event) {
        List<String> onlineInstanceList = event.getInstanceIds();
        if (ACTIVE_INSTANCE_SET.isEmpty() || onlineInstanceList.contains(currentInstanceId)) {
            return;
        }
        ACTIVE_INSTANCE_SET.addAll(onlineInstanceList);

        if (!CollectionUtils.isEmpty(ConnectHandler.CLIENT_MAP)) {
            List<String> clientIds = new ArrayList<>(ConnectHandler.CLIENT_MAP.keySet());
            Lists.partition(clientIds, SYNC_BATCH_SIZE_CLIENT_ID).forEach(clientIdList -> {
                log.info("发送当前MQTTX实例客户端连接数据: 实例ID=[{}], 数量=[{}]", currentInstanceId, clientIdList.size());
                internalMessageService.publish(
                        new InternalMessage<>(
                                SyncMsg.ofClientIds(currentInstanceId, clientIdList), System.currentTimeMillis(), brokerId), SYNC);
            });
        }

        List<ClientSub> clientSubs = new ArrayList<>(subscriptionService.getAllClientSubs());
        if (!CollectionUtils.isEmpty(clientSubs)) {
            Lists.partition(clientSubs,SYNC_BATCH_SIZE_CLIENT_SUB).forEach(clientSubList -> {
                log.info("发送当前MQTTX实例客户端订阅数据: 实例ID=[{}], 数量=[{}]", currentInstanceId, clientSubList.size());
                internalMessageService.publish(
                        new InternalMessage<>(
                                SyncMsg.ofClientSubs(currentInstanceId, clientSubList), System.currentTimeMillis(), brokerId), SYNC);
            });
        }
    }

    @EventListener(condition = "T(com.hycan.idn.mqttx.graceful.event.MqttxStateEvent).INSTANCE_SHUTDOWN.equals(#event.state)")
    public void instanceOffline(MqttxStateEvent event) {
        List<String> offlineInstanceList = event.getInstanceIds();
        if (offlineInstanceList.contains(currentInstanceId)) {
            return;
        }

        // 正常运行的节点收到服务停止通知，清理订阅、连接缓存
        for (String instanceId : offlineInstanceList) {
            ACTIVE_INSTANCE_SET.remove(instanceId);
            ConnectHandler.ALL_CLIENT_MAP.forEach((clientId, connectInfo) -> {
                if (Objects.nonNull(connectInfo) && connectInfo.getInstanceId().equals(instanceId)) {
                    subscriptionService.clearClientSubscriptions(clientId, true, false).subscribe();
                    if (enableSysTopic) {
                        subscriptionService.clearClientSysSub(clientId, false).subscribe();
                    }
                    ConnectHandler.ALL_CLIENT_MAP.remove(clientId);
                }
            });
        }
    }
}
