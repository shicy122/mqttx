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

import com.hycan.idn.mqttx.service.impl.SubscriptionServiceImpl;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 客户端订阅或解除订阅消息, 用于集群内部广播
 *
 * @author Shadow
 * @since 2.0.1
 */
@Data
public class ClientSubOrUnsubMsg {

    /**
     * 按顺序 -> 订阅，解除订阅
     */
    public static final int SUB = 1, UN_SUB = 2;

    private String clientId;

    private int qos;

    private String topic;

    private boolean cleanSession;

    /**
     * 当 {@code type} == 2, topics 不能为空
     */
    private List<String> topics;

    private boolean sysTopic;

    /**
     * Defined in {@link SubscriptionServiceImpl}
     * <ol>
     *     <li>1 -> 订阅</li>
     *     <li>2 -> 解除订阅</li>
     * </ol>
     */
    private int type;

    public static ClientSubOrUnsubMsg of(ClientSub clientSub, boolean isSysTopic, int type) {
        ClientSubOrUnsubMsg msg = new ClientSubOrUnsubMsg();
        msg.setClientId(clientSub.getClientId());
        msg.setQos(clientSub.getQos());
        msg.setTopic(clientSub.getTopic());
        msg.setCleanSession(clientSub.isCleanSession());
        msg.setSysTopic(isSysTopic);
        msg.setType(type);
        return msg;
    }

    public static ClientSubOrUnsubMsg of(String clientId, int qos, String topic, boolean cleanSession, boolean isSysTopic, int type) {
        ClientSubOrUnsubMsg msg = new ClientSubOrUnsubMsg();
        msg.setClientId(clientId);
        msg.setQos(qos);
        msg.setTopic(topic);
        msg.setCleanSession(cleanSession);
        msg.setSysTopic(isSysTopic);
        msg.setType(type);
        return msg;
    }

    public static ClientSubOrUnsubMsg of(String clientId, List<String> topics, boolean cleanSession, boolean isSysTopic, int type) {
        ClientSubOrUnsubMsg msg = new ClientSubOrUnsubMsg();
        msg.setClientId(clientId);
        msg.setTopics(topics);
        msg.setCleanSession(cleanSession);
        msg.setSysTopic(isSysTopic);
        msg.setType(type);
        return msg;
    }
}