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

import com.hycan.idn.mqttx.entity.MqttxOfflineMsg;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.pojo.Session;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * publish msg service
 *
 * @author Shadow
 * @since 2.0.1
 */
public interface IPublishMessageService {

    /**
     * 保存消息到DB
     *
     * @param pubMsg   publish 消息体
     * @param clientId 客户端id
     */
    Mono<Void> save2Db(String clientId, PubMsg pubMsg);

    /**
     * 保存消息到DB
     *
     * @param session {@link Session}
     * @param pubMsg  publish 消息体
     */
    void save2Cache(Session session, PubMsg pubMsg);

    /**
     * 清理与客户相关连的 publish 消息
     *
     * @param clientId 客户端id
     */
    Mono<Void> clear(String clientId);

    /**
     * 移除指定的 publish 消息
     *
     * @param clientId  客户端id
     * @param messageId 消息id
     */
    Mono<Void> remove(String clientId, int messageId);

    /**
     * 删除缓存中的 publish 消息
     *
     * @param clientId  客户端id
     * @param messageId 消息ID
     * @return publish 消息
     */
    PubMsg removeCache(String clientId, int messageId);

    /**
     * 获取客户关联的 publish message
     *
     * @param clientId 客户端id
     * @return 客户未能完成发送的消息列表
     */
    Flux<PubMsg> search(String clientId);

    /**
     * 客户端(cleanSession = false)上线，补发 qos1,2 消息
     *
     * @param pubMsg {@link PubMsg}
     */
    Mono<Void> republish(PubMsg pubMsg);
}
