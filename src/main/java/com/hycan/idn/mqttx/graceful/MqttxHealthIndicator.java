package com.hycan.idn.mqttx.graceful;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * 自定义健康检查，服务准备好之后，将健康状态设置成true
 *
 * @author shichongying
 * @datetime 2023-11-07 09:00
 */
@Component
public class MqttxHealthIndicator implements HealthIndicator {

    private boolean health = false;

    @Override
    public Health health() {
        if (health) {
            return Health.up().withDetail("Status", "Health").build();
        }
        return Health.down().withDetail("Status", "Not Health").build();
    }

    public void setHealth(boolean health) {
        this.health = health;
    }

    public boolean isHealth() {
        return health;
    }
}
