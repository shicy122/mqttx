package com.hycan.idn.mqttx.entity;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

/**
 * 存储保留消息
 *
 * @author shichongying
 * @datetime 2023年 01月 10日 15:27
 */
@Data
@Builder
@Document(collection = "mqttx_retain_msg")
public class MqttxRetainMsg {

    //@formatter:off

    @Id
    private String id;

    @Indexed(unique = true)
    @Field(name = "topic")
    private String topic;

    /**
     * <ol>
     *     <li>0 -> at most once</li>
     *     <li>1 -> at least once</li>
     *     <li>2 -> exactly once</li>
     * </ol>
     */
    @Field(name = "qos")
    private Integer qos;

    /**
     * sometimes we also named it <b>packetId</b>
     */
    @Field(name = "message_id")
    private Integer messageId;

    /** 发送的消息实体对象 **/
    @Field(name = "payload")
    private byte[] payload;

    /** 记录时间 **/
    @Field(name = "record_time")
    private LocalDateTime recordTime;

    //@formatter:on
}
