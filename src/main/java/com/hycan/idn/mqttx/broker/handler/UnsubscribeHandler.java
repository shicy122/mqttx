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
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
/**
 * {@link MqttMessageType#UNSUBSCRIBE} 取消订阅 消息处理器
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Handler(type = MqttMessageType.UNSUBSCRIBE)
public class UnsubscribeHandler extends AbstractMqttSessionHandler {

    private final Boolean enableSysTopic, enableLog;
    private final ISubscriptionService subscriptionService;

    public UnsubscribeHandler(MqttxConfig config, ISubscriptionService subscriptionService) {
        super(config.getCluster().getEnable());
        this.enableSysTopic = config.getSysTopic().getEnable();
        this.subscriptionService = subscriptionService;
        this.enableLog = config.getSysConfig().getEnableLog();
    }

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttUnsubscribeMessage mqttUnsubscribeMessage = (MqttUnsubscribeMessage) msg;
        int messageId = mqttUnsubscribeMessage.variableHeader().messageId();
        MqttUnsubscribePayload payload = mqttUnsubscribeMessage.payload();

        // 主题列表
        List<String> collect = new ArrayList<>(payload.topics());
        if (Boolean.TRUE.equals(enableLog)) {
            log.info("取消订阅: 客户端ID=[{}], Topic列表={}", clientId(ctx), collect);
        }

        if (Boolean.TRUE.equals(enableSysTopic)) {
            List<String> unSubSysTopics = collect.stream().filter(TopicUtils::isSys).toList();
            collect.removeAll(unSubSysTopics);
            Mono.when(unsubscribeSysTopics(unSubSysTopics, ctx), subscriptionService.unsubscribe(clientId(ctx), isCleanSession(ctx), collect))
                    .doOnSuccess(unused -> {
                        // response
                        MqttMessage mqttMessage = MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                MqttMessageIdVariableHeader.from(messageId),
                                null
                        );
                        ctx.writeAndFlush(mqttMessage);
                    }).subscribe();
            return;
        }

        subscriptionService.unsubscribe(clientId(ctx), isCleanSession(ctx), collect)
                .doOnSuccess(unused -> {
                    // response
                    MqttMessage mqttMessage = MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                            MqttMessageIdVariableHeader.from(messageId),
                            null
                    );
                    ctx.writeAndFlush(mqttMessage);
                }).subscribe();
    }

    /**
     * 系统主题订阅处理. 系统主题订阅没有持久化，仅保存在内存，需要单独处理.
     *
     * @param unSubSysTopics 解除订阅的主题列表
     * @param ctx            {@link ChannelHandlerContext}
     */
    private Mono<Void> unsubscribeSysTopics(List<String> unSubSysTopics, ChannelHandlerContext ctx) {
        return subscriptionService.unsubscribeSys(clientId(ctx), unSubSysTopics, false);
    }
}
