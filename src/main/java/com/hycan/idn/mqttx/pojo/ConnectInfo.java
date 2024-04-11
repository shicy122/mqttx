package com.hycan.idn.mqttx.pojo;

import lombok.Data;

/**
 * 连接信息
 *
 * @author Shadow
 * @datetime 2023-10-31 17:56
 */
@Data
public class ConnectInfo {

    /** 当前实例ID */
    private String instanceId;

    /** 时间戳，执行清理ALL_CLIENT_MAP时，用于判断先后顺序 */
    private long timestamp;

    public static ConnectInfo of(String instanceId, long timestamp) {
        ConnectInfo info = new ConnectInfo();
        info.setInstanceId(instanceId);
        info.setTimestamp(timestamp);
        return info;
    }
}
