package com.hycan.idn.mqttx.pojo;

import lombok.Data;

/**
 * 客户端上/下线集群消息
 *
 * @author shichongying
 * @datetime 2023年 03月 09日 10:57
 */
@Data
public class ClientConnOrDiscMsg {

    private static final String CLIENT_ONLINE = "connected";
    private static final String CLIENT_OFFLINE = "disconnected";

    /** 上/下线客户端ID */
    private String clientId;

    /** 当前实例ID */
    private String instanceId;

    /** 上/下线类型(ONLINE/OFFLINE) */
    private String type;

    /** 时间戳 */
    private long timestamp;

    public static ClientConnOrDiscMsg online(String clientId, String instanceId) {
        ClientConnOrDiscMsg msg = new ClientConnOrDiscMsg();
        msg.setClientId(clientId);
        msg.setInstanceId(instanceId);
        msg.setType(CLIENT_ONLINE);
        msg.setTimestamp(System.currentTimeMillis());
        return msg;
    }

    public static ClientConnOrDiscMsg offline(String clientId, String instanceId) {
        ClientConnOrDiscMsg msg = new ClientConnOrDiscMsg();
        msg.setClientId(clientId);
        msg.setInstanceId(instanceId);
        msg.setType(CLIENT_OFFLINE);
        msg.setTimestamp(System.currentTimeMillis());
        return msg;
    }
}
