package com.hycan.idn.mqttx.entity;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

/**
 * 客户端订阅信息
 *
 * @author shichongying
 * @datetime 2023年 01月 13日 14:09
 */
@Data
@Builder
@CompoundIndex(name = "client_id_1_topic_1", def = "{'client_id':1,'topic':1}", unique = true)
@Document(collection = "mqttx_subscribe")
public class MqttxSubscribe {

    //@formatter:off

    @Id
    private String id;

    /** 客户ID */
    @Indexed
    @Field(name = "client_id")
    private String clientId;

    /**
     * <ol>
     *     <li>0 -> at most once</li>
     *     <li>1 -> at least once</li>
     *     <li>2 -> exactly once</li>
     * </ol>
     */
    @Field(name = "qos")
    private Integer qos;

    /**  主题  **/
    @Indexed
    @Field(name = "topic")
    private String topic;

    /** 清理会话标志 */
    @Field(name = "clean_session")
    private Boolean cleanSession;

    /** 记录时间 **/
    @Field(name = "record_time")
    private LocalDateTime recordTime;

    //@formatter:on
}
