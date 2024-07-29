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

import com.hycan.idn.mqttx.entity.MqttxSession;
import com.hycan.idn.mqttx.service.IPubRelMessageService;
import com.hycan.idn.mqttx.utils.MessageIdUtil;
import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * MQTT 会话
 *
 * @author Shadow
 * @since 2.0.1
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Session {

    //@formatter:off

    public static final String KEY = "session";

    /**
     * mqtt 协议版本
     *
     * @see MqttVersion
     */
    private MqttVersion version;

    /** 客户ID */
    private String clientId;

    /** 清理会话标志 */
    private Boolean cleanSession;

    /** 用于 cleanSession 连接，负责存储 qos > 0 的消息 */
    private Map<Integer, PubMsg> pubMsgStore;

    /** @see IPubRelMessageService */
    private transient Set<Integer> outPubRelMsgStore;

    /** @see IPubRelMessageService */
    private transient Set<Integer> inPubRelMsgStore;

    /** 遗嘱消息 */
    private transient PubMsg willMessage;

    /** 用于生成 msgId */
    private int messageId = -1;
    //@formatter:on

    /**
     * 创建会话
     *
     * @param clientId     客户端 id
     * @param cleanSession clean session 标识. true: 1; false: 0
     * @return Session for clean session = 1
     */
    public static Session of(String clientId, boolean cleanSession) {
        return of(clientId, cleanSession, MqttVersion.MQTT_3_1_1);
    }

    /**
     * 创建会话
     *
     * @param clientId     客户端 id
     * @param cleanSession clean session 标识. true: 1; false: 0
     * @param version      mqtt 协议版本
     * @return Session for clean session = 1
     */
    public static Session of(String clientId, boolean cleanSession, MqttVersion version) {
        Session session = new Session();
        session.setClientId(clientId);
        session.setCleanSession(cleanSession);
        session.setVersion(version);
        if (cleanSession) {
            session.setPubMsgStore(new HashMap<>());
            session.setOutPubRelMsgStore(new HashSet<>());
            session.setInPubRelMsgStore(new HashSet<>());
        }
        return session;
    }

    public static Session of(MqttxSession session) {
        return Session.builder()
                .clientId(session.getClientId())
                .cleanSession(session.getCleanSession())
                .version(session.getVersion())
                .build();
    }

    /**
     * session 绑定 channel, 而 channel 绑定 EventLoop 线程，这个方法是线程安全的（如果没有额外的配置）。
     *
     * @return {@link #messageId}
     */
    public int increaseAndGetMessageId() {
        // SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0) Control Packets MUST contain a
        // non-zero 16-bit Packet Identifier [MQTT-2.3.1-1].
        if ((++messageId & 0xffff) != 0) {
            return MessageIdUtil.trimMessageId(messageId);
        }

        return MessageIdUtil.trimMessageId(++messageId);
    }

    /**
     * 清理遗嘱消息
     */
    public void clearWillMessage() {
        willMessage = null;
    }

    /**
     * 保存 {@link PubMsg}
     *
     * @param messageId 消息id
     * @param pubMsg    {@link PubMsg}
     */
    public void savePubMsg(Integer messageId, PubMsg pubMsg) {
        if (cleanSession) {
            pubMsg.setClientId(getClientId());
            pubMsgStore.put(messageId, pubMsg);
        }
    }

    /**
     * 移除 {@link PubMsg}
     *
     * @param messageId 消息id
     */
    public void removePubMsg(int messageId) {
        if (cleanSession) {
            pubMsgStore.remove(messageId);
        }
    }

    /**
     * 保存 {@link PubRelMsg}
     *
     * @param messageId 消息id
     */
    public void savePubRelInMsg(int messageId) {
        if (cleanSession) {
            inPubRelMsgStore.add(messageId);
        }
    }

    /**
     * 保存 {@link PubRelMsg}
     *
     * @param messageId 消息id
     */
    public void savePubRelOutMsg(int messageId) {
        if (cleanSession) {
            outPubRelMsgStore.add(messageId);
        }
    }

    /**
     * 移除 {@link PubRelMsg}
     *
     * @param messageId 消息id
     */
    public void removePubRelInMsg(int messageId) {
        if (cleanSession) {
            inPubRelMsgStore.remove(messageId);
        }
    }

    /**
     * 移除 {@link PubRelMsg}
     *
     * @param messageId 消息id
     */
    public void removePubRelOutMsg(int messageId) {
        if (cleanSession) {
            outPubRelMsgStore.remove(messageId);
        }
    }

    public boolean isDupMsg(int messageId) {
        return inPubRelMsgStore.contains(messageId);
    }
}