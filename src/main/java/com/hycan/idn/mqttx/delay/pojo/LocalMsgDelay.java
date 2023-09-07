package com.hycan.idn.mqttx.delay.pojo;

import com.hycan.idn.mqttx.pojo.Session;
import lombok.Data;

/**
 * 本地消息延迟队列(客户端cleanSession==true对应的消息处理)
 *
 * @author shichongying
 * @datetime 2023年 03月 15日 9:50
 */
@Data
public class LocalMsgDelay {

    private Session session;

    private int messageId;

    public static LocalMsgDelay of(Session session, int messageId) {
        LocalMsgDelay delay = new LocalMsgDelay();
        delay.setSession(session);
        delay.setMessageId(messageId);
        return delay;
    }
}
