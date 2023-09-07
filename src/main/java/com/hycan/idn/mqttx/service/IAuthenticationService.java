/*
 * Copyright 2002-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hycan.idn.mqttx.service;

import com.hycan.idn.mqttx.pojo.Authentication;
import com.hycan.idn.mqttx.pojo.ClientAuth;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 客户端认证服务
 *
 * @author Shadow
 * @since 2.0.1
 */
public interface IAuthenticationService {

    /**
     * 使用 {@link java.net.http.HttpClient} 发起请求
     *
     * @param authDTO {@link ClientAuth} 客户端认证对象
     */
    CompletableFuture<Authentication> asyncAuthenticate(ClientAuth authDTO);

    /**
     * 修改用户授权的 pub&sub topic 列表
     *
     * @param clientId            客户端 ID
     * @param authorizedSubTopics 被授权订阅 topic 列表
     * @param authorizedPubTopics 被授权发布 topic 列表
     */
    void alterUserAuthorizedTopic(String clientId, List<String> authorizedSubTopics, List<String> authorizedPubTopics);

    /**
     * 管理员客户端对应的密码续期
     *
     * @param clientId 管理员客户端ID
     */
    void incAdminClientExpireTime(String clientId);

    /**
     * 管理员客户端鉴权
     * @param authDTO authDTO {@link ClientAuth} 客户端认证对象
     * @return MQTT连接状态码
     */
    Mono<MqttConnectReturnCode> adminAuthenticate(ClientAuth authDTO);
}

