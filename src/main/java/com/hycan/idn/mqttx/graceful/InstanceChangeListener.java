package com.hycan.idn.mqttx.graceful;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.graceful.event.MqttxStateEvent;
import com.hycan.idn.mqttx.graceful.publisher.MqttxStatePublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

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
public class InstanceChangeListener extends Subscriber<InstancesChangeEvent> {

    public static final ConcurrentSkipListSet<String> ACTIVE_INSTANCE_SET = new ConcurrentSkipListSet<>();

    private final NacosDiscoveryProperties nacosDiscoveryProperties;

    private final MqttxStatePublisher publisher;

    private final boolean isClusterMode;

    public InstanceChangeListener(NacosDiscoveryProperties nacosDiscoveryProperties,
                                  MqttxConfig mqttxConfig,
                                  MqttxStatePublisher publisher) {
        this.nacosDiscoveryProperties = nacosDiscoveryProperties;

        this.isClusterMode = mqttxConfig.getCluster().getEnable();
        this.publisher = publisher;
    }

    @PostConstruct
    public void registerToNotifyCenter() {
        NotifyCenter.registerSubscriber(this);
    }

    @Override
    public Class<? extends Event> subscribeType() {
        return InstancesChangeEvent.class;
    }

    /**
     * 实例状态变化事件监听器
     *
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

        // 过滤出本服务的实例状态变化信息
        String serviceName = nacosDiscoveryProperties.getGroup() + "@@" + nacosDiscoveryProperties.getService();
        if (!Objects.equals(instancesChangeEvent.getServiceName(), serviceName)
                || !Objects.equals(instancesChangeEvent.getClusters(), nacosDiscoveryProperties.getClusterName())) {
            return;
        }

        // 获取Nacos通知的活跃实例列表
        List<String> activeInstanceList = instancesChangeEvent.getHosts().stream()
                .filter(instance -> instance.isEnabled() && instance.isHealthy())
                .map(Instance::getInstanceId).toList();

        // MQTTX实例下线
        if (activeInstanceList.size() < ACTIVE_INSTANCE_SET.size()) {
            List<String> offlineInstanceList = ACTIVE_INSTANCE_SET.stream()
                    .filter(instance -> !activeInstanceList.contains(instance)).toList();
            publisher.publish(MqttxStateEvent.INSTANCE_SHUTDOWN, offlineInstanceList);
            log.info("MQTTX实例下线: 下线实例列表={}, 下线后活跃实例列表={}", offlineInstanceList, ACTIVE_INSTANCE_SET);
        } else if (activeInstanceList.size() > ACTIVE_INSTANCE_SET.size()) {
            // 其他MQTTX实例上线
            List<String> onlineInstanceList = activeInstanceList.stream()
                    .filter(instance -> !ACTIVE_INSTANCE_SET.contains(instance)).toList();
            publisher.publish(MqttxStateEvent.INSTANCE_STARTUP, onlineInstanceList);
            log.info("MQTTX实例上线: 上线实例列表={}, 上线后活跃实例列表={}", onlineInstanceList, ACTIVE_INSTANCE_SET);
        }
    }
}