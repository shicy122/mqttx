package com.hycan.idn.mqttx.entity;

import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

/**
 * 存储 session
 *
 * @author shichongying
 * @datetime 2023年 01月 10日 15:29
 */
@Data
@Builder
@Document(collection = "mqttx_session")
public class MqttxSession {

    //@formatter:off

    @Id
    private String id;

    /**
     * mqtt 协议版本
     *
     * @see MqttVersion
     */
    @Field(name = "version")
    private MqttVersion version;

    /** 客户ID */
    @Indexed(unique = true)
    @Field(name = "client_id")
    private String clientId;

    /** 清理会话标志 */
    @Field(name = "clean_session")
    private Boolean cleanSession;

    /** 记录时间 **/
    @Field(name = "record_time")
    private LocalDateTime recordTime;

    //@formatter:on
}
