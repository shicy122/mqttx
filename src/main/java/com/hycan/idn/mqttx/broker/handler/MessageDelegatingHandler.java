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

import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.exception.AuthorizationException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 消息处理器, 消息将委派给 {@link Handler} 处理.
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Component
public class MessageDelegatingHandler {

    /**
     * 服务端需要处理的消息类别共10种：
     * <ol>
     *     <li>{@link MqttMessageType#CONNECT}</li>
     *     <li>{@link MqttMessageType#PUBLISH}</li>
     *     <li>{@link MqttMessageType#PUBACK}</li>
     *     <li>{@link MqttMessageType#PUBREC}</li>
     *     <li>{@link MqttMessageType#PUBREL}</li>
     *     <li>{@link MqttMessageType#PUBCOMP}</li>
     *     <li>{@link MqttMessageType#SUBSCRIBE}</li>
     *     <li>{@link MqttMessageType#UNSUBSCRIBE}</li>
     *     <li>{@link MqttMessageType#PINGREQ}</li>
     *     <li>{@link MqttMessageType#DISCONNECT}</li>
     * </ol>
     */
    private final Map<MqttMessageType, MqttMessageHandler> handlerMap = new HashMap<>(10);

    /**
     * 将处理器置入 {@link #handlerMap}
     *
     * @param mqttMessageHandlers 消息处理器
     */
    public MessageDelegatingHandler(List<MqttMessageHandler> mqttMessageHandlers) {
        Assert.notEmpty(mqttMessageHandlers, "messageHandlers can't be empty");

        // 置入处理器
        mqttMessageHandlers.forEach(mqttMessageHandler -> {
            Handler annotation = mqttMessageHandler.getClass().getAnnotation(Handler.class);
            Optional.ofNullable(annotation)
                    .map(Handler::type)
                    .ifPresent(mqttMessageType -> handlerMap.put(mqttMessageType, mqttMessageHandler));
        });

        // 保证初始化处理数量正常
        Assert.isTrue(handlerMap.size() == 10, "broker 消息处理器数量异常:" + handlerMap.size());
    }

    /**
     * 将消息委派给真正的 {@link MqttMessageHandler}
     *
     * @param ctx         {@link ChannelHandlerContext}
     * @param mqttMessage {@link MqttMessageType}
     */
    public void handle(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        MqttMessageType mqttMessageType = mqttMessage.fixedHeader().messageType();
        Session session = (Session) ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get();

        // 连接校验
        if (mqttMessageType != MqttMessageType.CONNECT) {
            if (session == null) {
                throw new AuthorizationException("session is null, access denied!");
            }

            final var channel = Optional.of(session.getClientId())
                    .map(ConnectHandler.CLIENT_MAP::get)
                    .map(BrokerHandler.CHANNELS::find)
                    .orElse(null);
            if (Objects.isNull(channel) || !channel.id().asShortText().equals(ctx.channel().id().asShortText())) {
                log.warn("旧连接未释放, 尝试重新断开, channel=[{}]", ctx.channel());
                ctx.close();
            }
        }

        Optional.of(handlerMap.get(mqttMessageType))
                .ifPresent(messageHandler -> messageHandler.process(ctx, mqttMessage));
    }
}