package com.hycan.idn.mqttx.service;

import com.hycan.idn.mqttx.pojo.ClientConnOrDiscMsg;
import reactor.core.publisher.Mono;

/**
 * Connect/Disconnect相关业务
 *
 * @author Shadow
 * @since 2.0.1
 */
public interface ISysMessageService {

    /**
     * 上线消息提醒
     *
     * @param msg 客户端上/下线集群消息
     * @return VOID
     */
    Mono<Void> onlineNotice(ClientConnOrDiscMsg msg);

    /**
     * 下线消息提醒
     *
     * @param msg 客户端上/下线集群消息
     * @return VOID
     */
    Mono<Void> offlineNotice(ClientConnOrDiscMsg msg);

    /**
     * 发送桥接的客户端上下线消息
     *
     * @param msg 客户端上/下线集群消息
     */
    void publishSysBridge(ClientConnOrDiscMsg msg);
}
