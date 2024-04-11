package com.hycan.idn.mqttx.graceful.event;

import lombok.Getter;
import lombok.Setter;
import org.springframework.context.ApplicationEvent;

import java.util.List;

/**
 * 实例上下线事件
 *
 * @author Shadow
 * @datetime 2023-11-10 13:41
 */
@Getter
@Setter
public class MqttxStateEvent extends ApplicationEvent {

    public static final String INSTANCE_STARTUP = "startup";
    public static final String INSTANCE_SHUTDOWN = "shutdown";

    private String state;
    private List<String> instanceIds;

    public MqttxStateEvent(Object source, String state, List<String> instanceIds) {
        super(source);

        this.state = state;
        this.instanceIds = instanceIds;
    }
}
