package com.hycan.idn.mqttx.service.impl;

import com.hycan.idn.mqttx.constants.MongoConstants;
import com.hycan.idn.mqttx.entity.MqttxRetainMsg;
import com.hycan.idn.mqttx.utils.TopicUtils;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.service.IRetainMessageService;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * 存储通过 redis 实现
 *
 * @author Shadow
 * @since 2.0.1
 */
@Service
public class RetainMessageServiceImpl implements IRetainMessageService {

    //@formatter:off

    private final ReactiveMongoTemplate mongoTemplate;

    //@formatter:on

    public RetainMessageServiceImpl(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;

        Assert.notNull(mongoTemplate, "mongoTemplate can't be null");
    }

    @Override
    public Flux<PubMsg> searchListByTopicFilter(String subTopic) {
        if (TopicUtils.isShare(subTopic)) {
            subTopic = subTopic.substring(TopicUtils.SHARE_TOPIC.length() + 1);
        }

        return get(subTopic);
    }

    @Override
    public Mono<Void> save(PubMsg pubMsg) {
        Query query = Query.query(Criteria.where(MongoConstants.TOPIC).is(pubMsg.getTopic()));
        Update update = Update.update(MongoConstants.RECORD_TIME, LocalDateTime.now())
                .set(MongoConstants.QOS, pubMsg.getQos())
                .set(MongoConstants.MESSAGE_ID, pubMsg.getMessageId())
                .set(MongoConstants.PAYLOAD, pubMsg.getPayload())
                .setOnInsert(MongoConstants.TOPIC, pubMsg.getTopic());

        return mongoTemplate.upsert(query, update, MqttxRetainMsg.class).then();
    }

    @Override
    public Mono<Void> remove(String topic) {
        Query query = Query.query(Criteria.where(MongoConstants.TOPIC).is(topic));
        return mongoTemplate.remove(query, MqttxRetainMsg.class).then();
    }

    @Override
    public Flux<PubMsg> get(String topic) {
        Query query = Query.query(Criteria.where(MongoConstants.TOPIC).is(topic));
        return mongoTemplate.find(query, MqttxRetainMsg.class).map(PubMsg::of);
    }
}
