package com.hycan.idn.mqttx.graceful.publisher;

import com.hycan.idn.mqttx.graceful.event.MqttxStateEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 实例状态变化事件发布者
 *
 * @author shichongying
 * @datetime 2023年 02月 26日 9:45
 */
@Component
public class MqttxStatePublisher {

    private final ApplicationContext applicationContext;

    public MqttxStatePublisher(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * 发布系统消息事件
     *
     * @param state       事件类型
     * @param instanceIds 实例ID列表
     */
    public void publish(String state, List<String> instanceIds) {
        applicationContext.publishEvent(new MqttxStateEvent(this, state, instanceIds));
    }
}