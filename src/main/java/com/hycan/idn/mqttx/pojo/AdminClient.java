package com.hycan.idn.mqttx.pojo;

import lombok.Data;

/**
 * 管理员客户端连接信息
 * @author shichongying
 */
@Data
public class AdminClient {

    private String clientId;

    private String instanceId;

    public static AdminClient of(String clientId, String instanceId) {
        AdminClient adminClient = new AdminClient();
        adminClient.setClientId(clientId);
        adminClient.setInstanceId(instanceId);
        return adminClient;
    }
}