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

package com.hycan.idn.mqttx.consumer;

import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.utils.Serializer;
import org.springframework.util.Assert;

import java.util.List;

/**
 * 该抽象类有两个子类 {@link InternalMessageSubscriber} 和 {@link InternalMessageSubscriber},
 * 具体采用哪个实现取决于用户配置 <code>mqttx.enable-cluster.type</code>
 */
public abstract class AbstractInnerChannel {

    private final String brokerId;
    private final Serializer serializer;
    private final List<Watcher> watchers;

    public AbstractInnerChannel(List<Watcher> watchers, Serializer serializer, MqttxConfig mqttxConfig) {
        Assert.notNull(watchers, "watchers can't be null");
        Assert.notNull(mqttxConfig, "mqttxConfig can't be null");
        Assert.notNull(serializer, "serializer can't be null");

        this.watchers = watchers;
        this.brokerId = mqttxConfig.getBrokerId();
        this.serializer = serializer;
    }


    /**
     * 分发集群消息，当前处理类别：
     *
     * @param message 消息内容
     * @param channel 订阅频道
     */
    @SuppressWarnings("rawtypes")
    public void dispatch(byte[] message, String channel) {
        // 同 broker 消息屏蔽
        InternalMessage internalMessage = serializer.deserialize(message, InternalMessage.class);
        if (brokerId.equals(internalMessage.getBrokerId())) {
            return;
        }

        for (Watcher watcher : watchers) {
            if (watcher.support(channel)) {
                watcher.action(message);
            }
        }
    }
}
