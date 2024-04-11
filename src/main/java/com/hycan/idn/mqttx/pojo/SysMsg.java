package com.hycan.idn.mqttx.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 系统消息
 *
 * @author Shadow
 * @datetime 2023-11-13 10:39
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SysMsg {

    /**
     * 订阅系统消息的客户端ID
     */
    private String clientId;

    /**
     * 订阅系统消息对应的topic
     */
    private String topic;

    /**
     * 客户端下线时间戳
     */
    private Long timestamp;

    public static SysMsg of(String clientId, String topic, Long timestamp) {
        SysMsg sysMsg = new SysMsg();
        sysMsg.setClientId(clientId);
        sysMsg.setTopic(topic);
        sysMsg.setTimestamp(timestamp);
        return sysMsg;
    }
}
