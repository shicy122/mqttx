package com.hycan.idn.mqttx.service.impl;

import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.constants.MongoConstants;
import com.hycan.idn.mqttx.entity.MqttxSession;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.ISessionService;
import com.hycan.idn.mqttx.utils.MessageIdUtil;
import com.mongodb.client.result.DeleteResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * 会话服务
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Service
public class SessionServiceImpl implements ISessionService {


    private final ReactiveStringRedisTemplate redisTemplate;
    private final ReactiveMongoTemplate mongoTemplate;
    private final String messageIdIncPrefix;

    public SessionServiceImpl(ReactiveStringRedisTemplate redisTemplate,
                              ReactiveMongoTemplate mongoTemplate,
                              MqttxConfig mqttxConfig) {
        this.mongoTemplate = mongoTemplate;
        this.redisTemplate = redisTemplate;

        this.messageIdIncPrefix = mqttxConfig.getRedis().getMessageIdIncPrefix();

        Assert.notNull(redisTemplate, "redisTemplate can't be null");
        Assert.notNull(mongoTemplate, "mongoTemplate can't be null");
    }

    @Override
    public Mono<Void> save(Session session) {
        Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(session.getClientId()));

        Update update = Update.update(MongoConstants.RECORD_TIME, LocalDateTime.now())
                .set(MongoConstants.VERSION, session.getVersion())
                .set(MongoConstants.CLEAN_SESSION, session.getCleanSession())
                .setOnInsert(MongoConstants.CLIENT_ID, session.getClientId());

        return mongoTemplate.upsert(query, update, MqttxSession.class).then();
    }

    @Override
    public Mono<Session> find(String clientId) {
        Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(clientId));
        return mongoTemplate.findOne(query, MqttxSession.class).map(Session::of);
    }

    @Override
    public Mono<Boolean> clear(String clientId) {
        Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(clientId));
        return redisTemplate.delete(messageIdIncPrefix + clientId)
                .then(mongoTemplate.remove(query, MqttxSession.class))
                .map(DeleteResult::wasAcknowledged);
    }

    @Override
    public Mono<Boolean> hasKey(String clientId) {
        Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(clientId));
        return mongoTemplate.exists(query, MqttxSession.class);
    }

    @Override
    public Mono<Integer> nextMessageId(String clientId) {
        return redisTemplate.opsForValue().increment(messageIdIncPrefix + clientId)
                .flatMap(e -> {
                    if ((e & 0xffff) == 0) {
                        return redisTemplate.opsForValue().increment(messageIdIncPrefix + clientId);
                    }
                    return Mono.just(e);
                })
                .map(MessageIdUtil::trimMessageId);
    }
}
