package com.hycan.idn.mqttx.pojo;

import io.netty.channel.ChannelId;
import lombok.Data;
import org.springframework.util.StringUtils;

import java.util.Objects;

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

    /** TCP Channel ID 字符串 */
    private String channelId;

    /** 当前实例ID */
    private String instanceId;

    /** 上/下线类型(ONLINE/OFFLINE) */
    private String type;

    /** 时间戳 */
    private long timestamp;

    public static ClientConnOrDiscMsg online(String clientId, ChannelId channelId, String instanceId) {
        ClientConnOrDiscMsg msg = new ClientConnOrDiscMsg();
        msg.setClientId(clientId);
        if (Objects.nonNull(channelId) && StringUtils.hasText(channelId.asShortText())) {
            msg.setChannelId(channelId.asShortText());
        }
        msg.setInstanceId(instanceId);
        msg.setType(CLIENT_ONLINE);
        msg.setTimestamp(System.currentTimeMillis() + 150L);
        return msg;
    }

    public static ClientConnOrDiscMsg offline(String clientId, ChannelId channelId, String instanceId) {
        ClientConnOrDiscMsg msg = new ClientConnOrDiscMsg();
        msg.setClientId(clientId);
        msg.setInstanceId(instanceId);
        if (Objects.nonNull(channelId) && StringUtils.hasText(channelId.asShortText())) {
            msg.setChannelId(channelId.asShortText());
        }
        msg.setType(CLIENT_OFFLINE);
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
