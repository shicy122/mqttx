package com.hycan.idn.mqttx.listener;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.consumer.Watcher;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.SyncMsg;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.utils.JSON;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 监听到实例状态发生变化时，向其他节点同步当前节点的连接信息
 *
 * @author shichongying
 */
@Slf4j
@DependsOn("instanceRegisteredListener")
@Component
public class InstanceChangeListener extends Subscriber<InstancesChangeEvent> implements Watcher {

    public static final ConcurrentSkipListSet<String> ACTIVE_INSTANCE_SET = new ConcurrentSkipListSet<>();

    /**
     * 内部消息发布服务
     */
    private final IInternalMessageService internalMessageService;

    private final NacosDiscoveryProperties nacosDiscoveryProperties;

    private final Serializer serializer;

    private final boolean isClusterMode;

    private final String brokerId, currentInstanceId, SYNC;

    public InstanceChangeListener(IInternalMessageService internalMessageService,
                                  NacosDiscoveryProperties nacosDiscoveryProperties,
                                  MqttxConfig mqttxConfig,
                                  Serializer serializer) {
        this.internalMessageService = internalMessageService;
        this.nacosDiscoveryProperties = nacosDiscoveryProperties;
        this.serializer = serializer;

        this.isClusterMode = mqttxConfig.getCluster().getEnable();
        this.brokerId = mqttxConfig.getBrokerId();
        this.currentInstanceId = mqttxConfig.getInstanceId();
        this.SYNC = mqttxConfig.getKafka().getSync();
    }


    @PostConstruct
    public void registerToNotifyCenter(){
        NotifyCenter.registerSubscriber(this);
    }

    /**
     * 实例状态变化事件监听器
     * @param instancesChangeEvent 实例注册事件
     */
    @Override
    public void onEvent(InstancesChangeEvent instancesChangeEvent) {
        if (!isClusterMode) {
            return;
        }

        if (Objects.isNull(instancesChangeEvent)) {
            return;
        }

        String serviceName = nacosDiscoveryProperties.getGroup() + "@@" + nacosDiscoveryProperties.getService();
        if (!Objects.equals(instancesChangeEvent.getServiceName(), serviceName)
                || !Objects.equals(instancesChangeEvent.getClusters(), nacosDiscoveryProperties.getClusterName())) {
            return;
        }

        // 获取当前活跃实例
        List<String> activeInstanceList = instancesChangeEvent.getHosts().stream()
                .filter(instance -> instance.isEnabled() && instance.isHealthy())
                .map(Instance::getInstanceId).toList();

        // 有实例下线
        if (activeInstanceList.size() < ACTIVE_INSTANCE_SET.size()) {
            List<String> offlineInstanceList = ACTIVE_INSTANCE_SET.stream()
                    .filter(instance -> !activeInstanceList.contains(instance)).toList();
            log.info("下线实例列表={}", offlineInstanceList);

            for (String instanceId : offlineInstanceList) {
                ACTIVE_INSTANCE_SET.remove(instanceId);
                ConnectHandler.ALL_CLIENT_MAP.forEach((key, value) -> {
                    if (value.equals(instanceId)) {
                        ConnectHandler.ALL_CLIENT_MAP.remove(key);
                    }
                });
            }
        }

        // 有实例上线
        if (activeInstanceList.size() > ACTIVE_INSTANCE_SET.size()) {
            List<String> onlineInstanceList = activeInstanceList.stream()
                    .filter(instance -> !ACTIVE_INSTANCE_SET.contains(instance)).toList();
            log.info("上线实例列表={}", onlineInstanceList);
            ACTIVE_INSTANCE_SET.addAll(onlineInstanceList);

            ConnectHandler.CLIENT_MAP.keys();
            if (!CollectionUtils.isEmpty(ConnectHandler.CLIENT_MAP)) {
                List<String> clientIds = ConnectHandler.CLIENT_MAP.keySet().stream().toList();
                log.info("发送当前实例客户端连接数据: 实例ID=[{}], 客户端数量=[{}]", currentInstanceId, clientIds.size());
                internalMessageService.publish(
                        new InternalMessage<>(
                                SyncMsg.of(currentInstanceId, clientIds), System.currentTimeMillis(), brokerId), SYNC);
            }
        }
    }

    @Override
    public Class<? extends Event> subscribeType() {
        return InstancesChangeEvent.class;
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
        List<String> clientIds = syncMsg.getClientIds();
        if (!StringUtils.hasText(instanceId) || CollectionUtils.isEmpty(clientIds)) {
            return;
        }

        log.info("接收其他实例客户端连接数据: 实例ID=[{}], 客户端数量=[{}]", currentInstanceId, clientIds.size());
        for (String clientId : syncMsg.getClientIds()) {
            ConnectHandler.ALL_CLIENT_MAP.put(clientId, instanceId);
        }
    }

    @Override
    public boolean support(String channel) {
        return SYNC.equals(channel);
    }
}