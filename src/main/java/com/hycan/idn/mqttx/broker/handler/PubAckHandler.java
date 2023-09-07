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

package com.hycan.idn.mqttx.broker.handler;

import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.service.IAuthenticationService;
import com.hycan.idn.mqttx.service.IPublishMessageService;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link MqttMessageType#PUBACK} 发布消息响应 消息处理器
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Handler(type = MqttMessageType.PUBACK)
public class PubAckHandler extends AbstractMqttSessionHandler {

    private final IPublishMessageService publishMessageService;

    private final IAuthenticationService authenticationService;

    public PubAckHandler(IPublishMessageService publishMessageService,
                         MqttxConfig config,
                         IAuthenticationService authenticationService) {
        super(config.getCluster().getEnable());
        this.publishMessageService = publishMessageService;
        this.authenticationService = authenticationService;
    }

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {
        authenticationService.incAdminClientExpireTime(clientId(ctx));

        MqttPubAckMessage mqttPubAckMessage = (MqttPubAckMessage) msg;
        int messageId = mqttPubAckMessage.variableHeader().messageId();
        if (isCleanSession(ctx)) {
            getSession(ctx).removePubMsg(messageId);
        } else {
            publishMessageService.remove(clientId(ctx), messageId).subscribe();
        }
    }
}
