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

package com.hycan.idn.mqttx.pojo;

import com.hycan.idn.mqttx.entity.MqttxOfflineMsg;
import com.hycan.idn.mqttx.entity.MqttxRetainMsg;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 发布的消息
 *
 * @author Shadow
 * @since 2.0.1
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PubMsg {
    //@formatter:off

    /**
     * 消息发给指定的 client
     */
    private String clientId;

    /**
     * <ol>
     *     <li>0 -> at most once</li>
     *     <li>1 -> at least once</li>
     *     <li>2 -> exactly once</li>
     * </ol>
     */
    private int qos;

    /**
     * sometimes we also named it <b>packetId</b>
     */
    private int messageId;

    /** 消息主题 **/
    private String topic;

    /** 消息是否为保留消息 */
    private boolean retain;

    /** 消息是否为遗嘱消息 */
    private boolean will;

    /** 是否为 dup 消息 */
    private boolean dup;

    private byte[] payload;

    //@formatter:on

    public static PubMsg of(int qos, String topic, byte[] payload) {
        return PubMsg.builder()
                .qos(qos)
                .topic(topic)
                .payload(payload)
                .build();
    }

    public static PubMsg of(int qos, String topic, boolean retain, byte[] payload) {
        return PubMsg.builder()
                .qos(qos)
                .topic(topic)
                .retain(retain)
                .payload(payload)
                .build();
    }

    public static PubMsg of(MqttxRetainMsg msg) {
        return PubMsg.builder()
                .retain(true)
                .qos(msg.getQos())
                .topic(msg.getTopic())
                .messageId(msg.getMessageId())
                .payload(msg.getPayload())
                .build();
    }

    public static PubMsg of(MqttxOfflineMsg msg) {
        return PubMsg.builder()
                .clientId(msg.getClientId())
                .qos(msg.getQos())
                .topic(msg.getTopic())
                .messageId(msg.getMessageId())
                .payload(msg.getPayload())
                .build();
    }
}