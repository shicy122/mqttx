package com.hycan.idn.mqttx.service.impl;

import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.utils.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * 基于 kafka 实现
 *
 * @author shichongying
 * @see InternalMessageServiceImpl
 * @since v1.0.6
 */
@Slf4j
@Service
public class InternalMessageServiceImpl implements IInternalMessageService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final Serializer serializer;

    public InternalMessageServiceImpl(KafkaTemplate<String, byte[]> kafkaTemplate,
                                      Serializer serializer) {
        this.kafkaTemplate = kafkaTemplate;
        this.serializer = serializer;
    }

    @Override
    public <T> void publish(InternalMessage<T> internalMessage, String channel) {
        kafkaTemplate.send(channel, serializer.serialize(internalMessage));
    }
}
