package com.hycan.idn.mqttx.service;

import reactor.core.publisher.Mono;

/**
 * 实例调度策略
 *
 * @author shichongying
 */
public interface IInstanceRouterService {

    /**
     * admin客户端连接路由策略，不满足时抛异常拒绝连接
     *
     * @param clientId 当前发起连接的admin客户端ID
     * @return true/false
     */
    Mono<Boolean> adminClientConnectRouter(String clientId);

    /**
     * 清理admin客户端连接信息
     * @param clientId 当前发起断开连接的admin客户端ID
     * @return VOID
     */
    Mono<Void> clearAdminClientConnect(String clientId);
}
