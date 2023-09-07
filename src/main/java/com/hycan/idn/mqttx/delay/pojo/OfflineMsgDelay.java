package com.hycan.idn.mqttx.delay.pojo;

import lombok.Data;

/**
 * 离线消息延迟队列(客户端cleanSession==false对应的消息处理)
 *
 * @author shichongying
 * @datetime 2023年 03月 15日 9:50
 */
@Data
public class OfflineMsgDelay {

    private String clientId;

    private int messageId;

    public static OfflineMsgDelay of(String clientId, int messageId) {
        OfflineMsgDelay delay = new OfflineMsgDelay();
        delay.setClientId(clientId);
        delay.setMessageId(messageId);
        return delay;
    }
}
