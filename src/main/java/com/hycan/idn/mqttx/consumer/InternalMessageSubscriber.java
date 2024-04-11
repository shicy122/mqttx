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
import com.hycan.idn.mqttx.utils.ExceptionUtil;
import com.hycan.idn.mqttx.utils.Serializer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;


/**
 * 集群消息订阅分发处理器, kafka 实现
 *
 * @author shichongying
 * @since v1.0.6
 */
@Slf4j
@Component
public class InternalMessageSubscriber extends AbstractInnerChannel implements
        BatchAcknowledgingMessageListener<String, byte[]>, ConsumerSeekAware {

    private volatile boolean onceFlag = false;

    public InternalMessageSubscriber(List<Watcher> watchers, Serializer serializer, MqttxConfig mqttxConfig) {
        super(watchers, serializer, mqttxConfig);
    }

    /**
     * 集群消息处理
     *
     * @param consumerRecords {@link ConsumerRecord}
     * @param ack             ACK
     */
    @Override
    @KafkaListener(
            topics = {
                    "#{@mqttxConfig.kafka.sys}",
                    "#{@mqttxConfig.kafka.sync}",
                    "#{@mqttxConfig.kafka.pub}",
                    "#{@mqttxConfig.kafka.pubAck}",
                    "#{@mqttxConfig.kafka.pubRec}",
                    "#{@mqttxConfig.kafka.pubCom}",
                    "#{@mqttxConfig.kafka.pubRel}",
                    "#{@mqttxConfig.kafka.connect}",
                    "#{@mqttxConfig.kafka.disconnect}",
                    "#{@mqttxConfig.kafka.authorized}",
                    "#{@mqttxConfig.kafka.subOrUnsub}"
            }, concurrency = "#{@mqttxConfig.kafka.concurrency}")
    public void onMessage(@NonNull List<ConsumerRecord<String, byte[]>> consumerRecords, @NonNull Acknowledgment ack) {
        if (CollectionUtils.isEmpty(consumerRecords)) {
            return;
        }

        try {
            consumerRecords.forEach(consumerRecord -> {
                byte[] value = consumerRecord.value();
                String topic = consumerRecord.topic();
                dispatch(value, topic);
            });
        } catch (Exception e) {
            log.error("处理Kafka消息异常={}", ExceptionUtil.getExceptionCause(e));
        } finally {
            ack.acknowledge();
        }
    }

    /**
     * 当 partition assignment 变化时，重置消费者的 offset 为 latest, 此举为避免集群消费者重新上线后消费之前的消息导致数据不一致的问题。
     * 仅在项目初始化时执行一次此函数.
     */
    @Override
    public void onPartitionsAssigned(@NonNull Map<TopicPartition, Long> assignments, @NonNull ConsumerSeekCallback callback) {
        if (!onceFlag) {
            onceFlag = true;
            callback.seekToEnd(assignments.keySet());
        }
    }
}
