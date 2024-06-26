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

import com.hycan.idn.mqttx.pojo.InternalMessage;

/**
 * 内部消息发布服务
 *
 * @author Shadow
 * @since 2.0.1
 */
public interface IInternalMessageService {

    /**
     * 发布集群消息
     *
     * @param internalMessage {@link InternalMessage}
     * @param <T>             {@link InternalMessage#getData()} 类别
     * @param channel         推送频道
     */
    <T> void publish(InternalMessage<T> internalMessage, String channel);
}