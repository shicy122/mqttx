package com.hycan.idn.mqttx.entity;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

/**
 * 存储MQTTX用户信息
 *
 * @author Shadow
 * @datetime 2024-02-19 14:58
 */
@Data
@Builder
@Document(collection = "mqttx_user")
public class MqttxUser {

    //@formatter:off

    @Id
    private String id;

    /** MQTT用户名 */
    private String username;
    /** mqtt鉴权密码 */
    private String password;
    /** 加密的盐 */
    private String salt;
    /** 是否为超级用户 */
    private Integer isSuperuser;
    /** 创建时间 */
    private LocalDateTime recordTime;

    //@formatter:on
}
