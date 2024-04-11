package com.hycan.idn.mqttx.graceful;

import com.alibaba.cloud.nacos.registry.NacosAutoServiceRegistration;
import com.alibaba.cloud.nacos.registry.NacosServiceRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * 监听nacos注册成功事件，将实例状态变更为已上线
 * 解决微服务优雅下线时 pod restart 同一个ip地址导致重新注册上去后为下线状态  需要更新为上线状态
 *
 * @author shichongying
 */
@Slf4j
@Component
public class InstanceRegisteredListener {

    private static final String STATUS_UP = "UP";

    private final Registration registration;

    private final NacosServiceRegistry nacosServiceRegistry;

    public InstanceRegisteredListener(Registration registration,
                                      NacosServiceRegistry nacosServiceRegistry) {
        this.registration = registration;
        this.nacosServiceRegistry = nacosServiceRegistry;
    }

    /**
     * 监听到实例注册事件时，将实例状态更新为上线（针对有状态服务）
     * @param instanceRegisteredEvent 实例注册事件
     */
    @EventListener
    public void listen(InstanceRegisteredEvent<NacosAutoServiceRegistration> instanceRegisteredEvent) {
        if (Objects.isNull(instanceRegisteredEvent)) {
            return;
        }

        nacosServiceRegistry.setStatus(registration, STATUS_UP);
    }
}