package com.hycan.idn.mqttx.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据同步消息结构体
 *
 * @author shichongying
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SyncMsg {

    private String instanceId;

    private List<String> clientIds;

    private List<ClientSub> clientSubs;

    public static SyncMsg ofClientIds(String instanceId, List<String> clientIds) {
        SyncMsg syncMsg = new SyncMsg();
        syncMsg.setInstanceId(instanceId);
        syncMsg.setClientIds(new ArrayList<>(clientIds));
        return syncMsg;
    }

    public static SyncMsg ofClientSubs(String instanceId, List<ClientSub> clientSubs) {
        SyncMsg syncMsg = new SyncMsg();
        syncMsg.setInstanceId(instanceId);
        syncMsg.setClientSubs(new ArrayList<>(clientSubs));
        return syncMsg;
    }
}
