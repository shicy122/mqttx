package com.hycan.idn.mqttx.controller.pojo;

import com.hycan.idn.mqttx.pojo.ClientSub;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * 客户端信息响应VO
 *
 * @author Shadow
 * @datetime 2023-11-02 16:03
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MqttClientInfoRspVO {

    private String channelId;

    private Boolean isSync;

    private List<ClientSub> clientSubs = new ArrayList<>();
}
