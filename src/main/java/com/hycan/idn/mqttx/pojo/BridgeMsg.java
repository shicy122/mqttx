package com.hycan.idn.mqttx.pojo;

import lombok.Data;

import java.io.Serializable;

/**
 * 桥接消息
 *
 * @author shichongying
 * @datetime 2023年 03月 22日 19:02
 */
@Data
public class BridgeMsg implements Serializable {

    private static final long serialVersionUID = 4967108846565995342L;

    private String topic;
    private byte[] payload;

    public static BridgeMsg of(String topic, byte[] payload) {
        BridgeMsg msg = new BridgeMsg();
        msg.setTopic(topic);
        msg.setPayload(payload);
        return msg;
    }
}
