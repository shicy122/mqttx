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

import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.IAuthenticationService;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link MqttMessageType#PINGREQ} 消息处理器
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Handler(type = MqttMessageType.PINGREQ)
public class PingReqHandler implements MqttMessageHandler {

    private final IAuthenticationService authenticationService;

    public PingReqHandler(IAuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {
        Session session = (Session) ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get();
        authenticationService.incAdminClientExpireTime(session.getClientId());

        MqttMessage mqttMessage = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                null, null);
        ctx.writeAndFlush(mqttMessage);
    }
}