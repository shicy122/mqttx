package com.hycan.idn.mqttx.entity;

import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.utils.BytesUtil;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

/**
 * 存储离线消息
 *
 * @author shichongying
 * @datetime 2023年 01月 10日 15:27
 */
@Data
@Builder
@CompoundIndex(name = "client_id_1_message_id_1", def = "{'client_id':1,'message_id':1}", unique = true)
@Document(collection = "mqttx_offline_msg")
public class MqttxOfflineMsg {

    //@formatter:off

    @Id
    private String id;

    /** 订阅的客户端ID **/
    @Field(name = "client_id")
    private String clientId;

    /**  主题  **/
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
    @Indexed(name = "delete_at", expireAfterSeconds = 30 * 24 * 60 * 60)
    @Field(name = "record_time")
    private LocalDateTime recordTime;

    @Field(name = "retry_time")
    private LocalDateTime retryTime;

    //@formatter:on


    @Override
    public String toString() {
        return "MqttxOfflineMsg{" +
                "clientId='" + clientId + '\'' +
                ", topic='" + topic + '\'' +
                ", qos=" + qos +
                ", messageId=" + messageId +
                ", payload=" + BytesUtil.bytesToHexString(payload) +
                ", recordTime=" + recordTime +
                ", retryTime=" + retryTime +
                '}';
    }

    public static MqttxOfflineMsg of(PubMsg pubMsg) {
        return MqttxOfflineMsg.builder()
                .clientId(pubMsg.getClientId())
                .topic(pubMsg.getTopic())
                .qos(pubMsg.getQos())
                .messageId(pubMsg.getMessageId())
                .payload(pubMsg.getPayload())
                .recordTime(LocalDateTime.now())
                .retryTime(LocalDateTime.now())
                .build();
    }
}
