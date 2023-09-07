package com.hycan.idn.mqttx.pojo;

import lombok.Data;

import java.util.List;

/**
 * 数据同步消息结构体
 *
 * @author shichongying
 */
@Data
public class SyncMsg {

    private String instanceId;

    private List<String> clientIds;

    public static SyncMsg of(String instanceId, List<String> clientIds) {
        SyncMsg syncMsg = new SyncMsg();
        syncMsg.setInstanceId(instanceId);
        syncMsg.setClientIds(clientIds);
        return syncMsg;
    }
}
