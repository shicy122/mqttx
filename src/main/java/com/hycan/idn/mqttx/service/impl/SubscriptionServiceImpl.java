package com.hycan.idn.mqttx.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.constants.MongoConstants;
import com.hycan.idn.mqttx.constants.ShareStrategyEnum;
import com.hycan.idn.mqttx.consumer.Watcher;
import com.hycan.idn.mqttx.entity.MqttxSubscribe;
import com.hycan.idn.mqttx.pojo.ClientSub;
import com.hycan.idn.mqttx.pojo.ClientSubOrUnsubMsg;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import com.hycan.idn.mqttx.utils.TopicUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.hycan.idn.mqttx.constants.ShareStrategyEnum.hash;
import static com.hycan.idn.mqttx.constants.ShareStrategyEnum.random;
import static com.hycan.idn.mqttx.constants.ShareStrategyEnum.round;

/**
 * 主题订阅服务.
 * 为了优化 cleanSession = 1 会话的性能，所有与之相关的状态均保存在内存当中.
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Service
public class SubscriptionServiceImpl implements ISubscriptionService, Watcher {

    //@formatter:off

    private static final int ASSUME_COUNT = 100_000;
    /**
     * 按顺序 -> 订阅，解除订阅，删除 topic
     */
    private static final int SUB = 1, UN_SUB = 2;
    private final ReactiveMongoTemplate mongoTemplate;
    private final Serializer serializer;
    private final IInternalMessageService internalMessageService;
    /**
     * 共享主题轮询策略
     */
    private final ShareStrategyEnum shareStrategyEnum;
    private final boolean enableCluster, enableShareTopic, enableFilterTopic;
    private final String brokerId, topicPrefix, SUB_UNSUB;
    /**
     * 共享订阅轮询，存储轮询参数
     */
    private Map<String, AtomicInteger> roundMap;

    /**
     * 包含通配符 # 和 + 和 $share 的 topic 集合，存储于内存中
     */
    private final ConcurrentSkipListSet<String> incWildcardOrShareTopics = new ConcurrentSkipListSet<>();
    /**
     * 不含通配符 # 和 + 和 $share 的 topic 集合，存储于内存中
     */
    private final ConcurrentSkipListSet<String> nonWildcardOrShareTopics = new ConcurrentSkipListSet<>();
    /**
     * topic -> clients 关系集合，clients 不区分 cleanSession 和 通配符
     */
    private final Map<String, Set<ClientSub>> allTopicClientsMap = new ConcurrentHashMap<>(ASSUME_COUNT);
    /**
     * client -> topics 关系集合，仅缓存 cleanSession == true 对应的 topics
     */
    private final Map<String, Set<String>> inMemClientTopicsMap = new ConcurrentHashMap<>(ASSUME_COUNT);

    /**
     * 系统主题 -> clients map
     */
    private final Map<String, Set<ClientSub>> sysTopicClientsMap = new ConcurrentHashMap<>();

    //@formatter:on
    public SubscriptionServiceImpl(ReactiveMongoTemplate mongoTemplate,
                                   MqttxConfig mqttxConfig,
                                   Serializer serializer,
                                   @Nullable IInternalMessageService internalMessageService) {
        Assert.notNull(mongoTemplate, "mongoTemplate can't be null");

        this.mongoTemplate = mongoTemplate;
        this.serializer = serializer;
        this.internalMessageService = internalMessageService;

        MqttxConfig.ShareTopic shareTopic = mqttxConfig.getShareTopic();
        this.enableShareTopic = shareTopic.getEnable();
        this.shareStrategyEnum = shareTopic.getShareSubStrategy();
        if (round == shareStrategyEnum) {
            roundMap = new ConcurrentHashMap<>();
        }

        MqttxConfig.Cluster cluster = mqttxConfig.getCluster();
        this.enableCluster = cluster.getEnable();
        this.brokerId = mqttxConfig.getBrokerId();
        this.SUB_UNSUB = mqttxConfig.getKafka().getSubOrUnsub();

        MqttxConfig.FilterTopic filterTopic = mqttxConfig.getFilterTopic();
        this.enableFilterTopic = filterTopic.getEnable();
        this.topicPrefix = filterTopic.getTopicPrefix();

        initInnerCache();
    }

    /**
     * 初始化内部缓存。目前的策略是全部加载，其实可以按需加载，按业务需求来吧。
     */
    private void initInnerCache() {
        log.info("开始加载缓存...");

        mongoTemplate.findAll(MqttxSubscribe.class)
                .collectList()
                .doOnSuccess(subs -> {
                    for (var sub : subs) {
                        String topic = sub.getTopic();
                        String clientId = sub.getClientId();
                        int qos = sub.getQos();

                        if (TopicUtils.isWildcard(topic) || TopicUtils.isShare(topic)) {
                            incWildcardOrShareTopics.add(topic);
                        } else {
                            nonWildcardOrShareTopics.add(topic);
                        }

                        allTopicClientsMap.computeIfAbsent(topic, s -> ConcurrentHashMap.newKeySet())
                                .add(ClientSub.of(clientId, qos, topic, false));
                    }
                })
                .doOnError(throwable -> log.error(throwable.getMessage(), throwable))
                // 注意: 这里必须block
                .block();

        log.info("结束加载缓存, Topic总数 = {}, 含通配符和共享Topic数量 = {}, 完整路径Topic数量 = {}",
                allTopicClientsMap.size(), incWildcardOrShareTopics.size(), nonWildcardOrShareTopics.size());
    }

    /**
     * 订阅主题
     *
     * @param clientSub 客户订阅信息
     */
    @Override
    public Mono<Void> subscribe(ClientSub clientSub) {
        String topic = clientSub.getTopic();
        String clientId = clientSub.getClientId();
        int qos = clientSub.getQos();
        boolean cleanSession = clientSub.isCleanSession();

        // 保存订阅关系到本地缓存
        subscribeWithCache(clientSub);

        // 广播订阅事件
        if (enableCluster) {
            InternalMessage<ClientSubOrUnsubMsg> im = new InternalMessage<>(
                    ClientSubOrUnsubMsg.of(clientId, qos, topic, cleanSession, false, SUB),
                    System.currentTimeMillis(), brokerId);
            internalMessageService.publish(im, SUB_UNSUB);
        }

        // cleanSession = false 保存订阅关系到 mongo
        if (!cleanSession && !TopicUtils.isSys(topic)) {
            Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(clientId)
                    .and(MongoConstants.TOPIC).is(topic));

            Update update = Update.update(MongoConstants.RECORD_TIME, LocalDateTime.now())
                    .set(MongoConstants.QOS, qos)
                    .set(MongoConstants.CLEAN_SESSION, false)
                    .setOnInsert(MongoConstants.TOPIC, topic)
                    .setOnInsert(MongoConstants.CLIENT_ID, clientId);
            return mongoTemplate.upsert(query, update, MqttxSubscribe.class).then();
        }

        return Mono.empty();
    }

    /**
     * 解除订阅
     *
     * @param clientId     客户id
     * @param cleanSession clientId 关联会话 cleanSession 状态
     * @param topics       主题列表
     */
    @Override
    public Mono<Void> unsubscribe(String clientId, boolean cleanSession, List<String> topics) {
        if (CollectionUtils.isEmpty(topics)) {
            return Mono.empty();
        }

        // 将订阅关系从本地缓存移除
        unsubscribeWithCache(clientId, topics);

        // 集群广播
        if (enableCluster) {
            InternalMessage<ClientSubOrUnsubMsg> im = new InternalMessage<>(
                    ClientSubOrUnsubMsg.of(clientId, topics, cleanSession, false, UN_SUB),
                    System.currentTimeMillis(), brokerId);
            internalMessageService.publish(im, SUB_UNSUB);
        }

        if (!cleanSession) {
            Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(clientId)
                    .and(MongoConstants.TOPIC).in(topics));
            return mongoTemplate.remove(query, MqttxSubscribe.class).then();
        }

        return Mono.empty();
    }

    /**
     * 返回订阅主题的客户列表。考虑到 pub 类别的消息最为频繁且每次 pub 都会触发 <code>searchSubscribeClientList(String topic)</code>
     * 方法，所以增加内部缓存以优化该方法的执行逻辑。
     *
     * @param topic 主题
     * @return 客户ID列表
     */
    @Override
    public Flux<ClientSub> searchSubscribeClientList(String topic) {
        // result
        List<ClientSub> clientSubList = new ArrayList<>();

        if (nonWildcardOrShareTopics.contains(topic)) {
            allTopicClientsMap.computeIfPresent(topic, (k, clientSubs) -> {
                clientSubList.addAll(clientSubs);
                return clientSubs;
            });
        }

        if (!enableFilterTopic || topic.startsWith(topicPrefix)) {
            List<ClientSub> shareClientSubList = new ArrayList<>();
            for (String subTopic : incWildcardOrShareTopics) {
                if (TopicUtils.match(topic, subTopic)) {
                    allTopicClientsMap.computeIfPresent(subTopic, (k, clientSubs) -> {
                        // 开启共享订阅 且 topic 是 $share 开头，加入到 shareClientSubList 中
                        if (enableShareTopic && TopicUtils.isShare(subTopic)) {
                            shareClientSubList.addAll(clientSubs);
                        } else {
                            clientSubList.addAll(clientSubs);
                        }
                        return clientSubs;
                    });
                }
            }

            ClientSub clientSub = chooseShareClient(shareClientSubList, topic);
            if(Objects.nonNull(clientSub)) {
                clientSubList.add(clientSub);
            }
        }

        return Flux.fromIterable(clientSubList);
    }

    /**
     * 共享订阅选择客户端：
     * 1、优先从列表中查找与当前 mqttx 建链的 client，减少消息发布耗时
     * 2、列表中没有与当前 mqttx 建链的client，则按以下策略选择
     *
     * @param clientSubList 接收客户端列表
     * @param topic         发布消息的topic
     * @return 按规则选择的客户端
     */
    private ClientSub chooseShareClient(List<ClientSub> clientSubList, String topic) {
        // 1 优先返回与当前 mqttx 建链的 client
        List<ClientSub> currentPodClientSubs = clientSubList.stream()
                .filter(clientSub -> ConnectHandler.CLIENT_MAP.containsKey(clientSub.getClientId())).toList();
        if (!CollectionUtils.isEmpty(currentPodClientSubs)) {
            return chooseStrategy(new ArrayList<>(currentPodClientSubs), topic);
        }

        // 2 查找状态为在线的 client
        List<ClientSub> onlineClientSubs = clientSubList.stream()
                .filter(clientSub -> ConnectHandler.ALL_CLIENT_MAP.containsKey(clientSub.getClientId())).toList();
        if (!CollectionUtils.isEmpty(onlineClientSubs)) {
            return chooseStrategy(new ArrayList<>(onlineClientSubs), topic);
        }

        // 3 按配置的策略选择 client
        return chooseStrategy(clientSubList, topic);
    }

    /**
     * <ol>
     *     <li>随机: {@link ShareStrategyEnum#random}</li>
     *     <li>哈希: {@link ShareStrategyEnum#hash}</li>
     *     <li>轮询: {@link ShareStrategyEnum#round}</li>
     * </ol>
     *
     * @param clientSubList clientSubList 接收客户端列表
     * @param topic         发布消息的topic
     * @return 按规则选择的客户端
     */
    private ClientSub chooseStrategy(List<ClientSub> clientSubList, String topic) {
        if (CollectionUtils.isEmpty(clientSubList)) {
            return null;
        }

        if (clientSubList.size() == 1) {
            return clientSubList.get(0);
        }

        // 集合排序
        clientSubList.sort(ClientSub::compareTo);


        if (hash == shareStrategyEnum) {
            return clientSubList.get(topic.hashCode() % clientSubList.size());
        } else if (random == shareStrategyEnum) {
            int key = ThreadLocalRandom.current().nextInt(0, clientSubList.size());
            return clientSubList.get(key % clientSubList.size());
        } else if (round == shareStrategyEnum) {
            int i = roundMap.computeIfAbsent(topic, s -> new AtomicInteger(0)).getAndIncrement();
            return clientSubList.get(i % clientSubList.size());
        }

        throw new IllegalArgumentException("不可能到达的代码, strategy:" + shareStrategyEnum);
    }

    @Override
    public Mono<Void> clearClientSubscriptions(String clientId, boolean cleanSession) {
        // cleanSession == true 则从缓存中取出满足条件的 topics
        if (cleanSession) {
            Set<String> keys = inMemClientTopicsMap.remove(clientId);
            if (CollectionUtils.isEmpty(keys)) {
                return Mono.empty();
            }
            return unsubscribe(clientId, true, new ArrayList<>(keys));
        } else {
            Query query = Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(clientId));

            return mongoTemplate.find(query, MqttxSubscribe.class)
                    .map(MqttxSubscribe::getTopic)
                    .collectList()
                    .flatMap(topics -> unsubscribe(clientId, false, new ArrayList<>(topics)));
        }
    }

    @Override
    public Mono<Void> clearUnAuthorizedClientSub(String clientId, List<String> authorizedSub) {
        Set<String> collect = nonWildcardOrShareTopics.stream()
                .filter(topic -> !authorizedSub.contains(topic)).collect(Collectors.toSet());

        collect.addAll(incWildcardOrShareTopics.stream()
                .filter(topic -> !authorizedSub.contains(topic)).collect(Collectors.toSet()));

        return Mono.when(
                unsubscribe(clientId, false, new ArrayList<>(collect)),
                unsubscribe(clientId, true, new ArrayList<>(collect)));
    }

    @Override
    public void action(byte[] msg) {
        InternalMessage<ClientSubOrUnsubMsg> im;
        if (serializer instanceof JsonSerializer se) {
            im = se.deserialize(msg, new TypeReference<>() {
            });
        } else {
            //noinspection unchecked
            im = serializer.deserialize(msg, InternalMessage.class);
        }

        ClientSubOrUnsubMsg data = im.getData();
        final int type = data.getType();
        final boolean isSysTopic = data.isSysTopic();

        switch (data.getType()) {
            case SUB -> {
                ClientSub clientSub = ClientSub.of(data.getClientId(), data.getQos(), data.getTopic(), data.isCleanSession());
                if (isSysTopic) {
                    subscribeSys(clientSub, true).subscribe();
                } else {
                    subscribeWithCache(clientSub);
                }
            }
            case UN_SUB -> {
                String clientId = data.getClientId();
                List<String> topics = data.getTopics();
                if (isSysTopic) {
                    unsubscribeSys(clientId, topics, true).subscribe();
                } else {
                    unsubscribeWithCache(data.getClientId(), data.getTopics());
                }
            }
            default -> log.error("非法的 ClientSubOrUnsubMsg type:" + type);
        }
    }

    @Override
    public boolean support(String channel) {
        return SUB_UNSUB.equals(channel);
    }

    /**
     * 将客户端订阅存储到缓存
     *
     * @param clientSub 客户端端订阅
     */
    private void subscribeWithCache(ClientSub clientSub) {
        String topic = clientSub.getTopic();
        String clientId = clientSub.getClientId();
        log.debug("缓存订阅信息: 客户端ID=[{}], Topic=[{}]", clientId, topic);

        if (TopicUtils.isWildcard(topic) || TopicUtils.isShare(topic)) {
            incWildcardOrShareTopics.add(topic);
        } else {
            nonWildcardOrShareTopics.add(topic);
        }
        allTopicClientsMap.computeIfAbsent(topic, s -> ConcurrentHashMap.newKeySet()).add(clientSub);

        if (clientSub.isCleanSession()) {
            inMemClientTopicsMap.computeIfAbsent(clientId, s -> ConcurrentHashMap.newKeySet()).add(topic);
        }
    }

    /**
     * 移除缓存中的订阅
     *
     * @param clientId 客户端ID
     * @param topics   主题列表
     */
    private void unsubscribeWithCache(String clientId, List<String> topics) {
        log.debug("移除订阅信息: 客户端ID=[{}], Topic列表=[{}]", clientId, topics);
        topics.forEach(topic -> {
            Set<ClientSub> clientSubs = allTopicClientsMap.get(topic);
            if (!CollectionUtils.isEmpty(clientSubs)) {
                allTopicClientsMap.get(topic).stream()
                        .filter(clientSub -> clientSub.getClientId().equals(clientId))
                        .forEach(clientSubs::remove);
            }

            if (CollectionUtils.isEmpty(allTopicClientsMap.get(topic))) {
                allTopicClientsMap.remove(topic);
                if (TopicUtils.isWildcard(topic) || TopicUtils.isShare(topic)) {
                    incWildcardOrShareTopics.remove(topic);
                } else {
                    nonWildcardOrShareTopics.remove(topic);
                }
            }
        });

        Optional.ofNullable(inMemClientTopicsMap.get(clientId)).ifPresent(topicList -> topicList.removeAll(topics));
    }

    @Override
    public Flux<ClientSub> searchSysTopicClients(String topic) {
        // result
        List<ClientSub> clientSubList = new ArrayList<>();
        List<ClientSub> shareClientSubList = new ArrayList<>();
        sysTopicClientsMap.forEach((wildTopic, set) -> {
            if (TopicUtils.match(topic, wildTopic)) {
                // 开启共享订阅 且 topic 是 $share 开头，加入到 shareClientSubList 中
                if (enableShareTopic && TopicUtils.isShare(wildTopic)) {
                    shareClientSubList.addAll(set);
                } else {
                    clientSubList.addAll(set);
                }
            }
        });

        ClientSub clientSub = chooseShareClient(shareClientSubList, topic);
        if(Objects.nonNull(clientSub)) {
            clientSubList.add(clientSub);
        }

        return Flux.fromIterable(clientSubList);
    }

    @Override
    public Mono<Void> subscribeSys(ClientSub clientSub, boolean isClusterMessage) {
        sysTopicClientsMap.computeIfAbsent(clientSub.getTopic(), k -> ConcurrentHashMap.newKeySet()).add(clientSub);

        // 集群广播
        String topic = clientSub.getTopic();
        if (enableCluster && !isClusterMessage && TopicUtils.isSysEvent(topic)) {
            InternalMessage<ClientSubOrUnsubMsg> im = new InternalMessage<>(
                    ClientSubOrUnsubMsg.of(clientSub.getClientId(), 0, topic, false, true, SUB),
                    System.currentTimeMillis(), brokerId);
            internalMessageService.publish(im, SUB_UNSUB);
        }

        return Mono.empty();
    }

    @Override
    public Mono<Void> unsubscribeSys(String clientId, List<String> topics, boolean isClusterMessage) {
        for (String topic : topics) {
            Set<ClientSub> clientSubs = sysTopicClientsMap.get(topic);
            if (!CollectionUtils.isEmpty(clientSubs)) {
                clientSubs.remove(ClientSub.of(clientId, 0, topic, false));
            }
        }

        // 集群广播
        if (enableCluster && !isClusterMessage) {
            InternalMessage<ClientSubOrUnsubMsg> im = new InternalMessage<>(
                    ClientSubOrUnsubMsg.of(clientId, topics, false, true, UN_SUB),
                    System.currentTimeMillis(), brokerId);
            internalMessageService.publish(im, SUB_UNSUB);
        }

        return Mono.empty();
    }

    @Override
    public Mono<Void> clearClientSysSub(String clientId) {
        List<String> topics = sysTopicClientsMap.keySet().stream().toList();
        return unsubscribeSys(clientId, topics, false);
    }
}
