package com.hycan.idn.mqttx.delay.exec;

import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.constants.MongoConstants;
import com.hycan.idn.mqttx.delay.DelayQueueManager;
import com.hycan.idn.mqttx.delay.pojo.OfflineMsgDelay;
import com.hycan.idn.mqttx.entity.MqttxOfflineMsg;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.service.IPublishMessageService;
import com.hycan.idn.mqttx.utils.ExceptionUtil;
import com.hycan.idn.mqttx.utils.ThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 发布消息本地延迟队列
 *
 * @author shichongying
 * @datetime 2023年 02月 07日 20:20
 */
@Slf4j
@Component
public class OfflineMsgDelayQueueExecutor extends DelayQueueManager<OfflineMsgDelay> {

    private static final long MAX_LOG_INTERVAL_TIME = 1000; //预设的最大日志时间（毫秒）
    private static AtomicLong lastLogTime = new AtomicLong(0L); //上一条日志的时间戳

    private final ReactiveMongoTemplate mongoTemplate;
    private final IPublishMessageService publishMessageService;

    private final int batchSavePubMsgPoolSize, batchSavePubMsgQueueSize, batchSavePubMsgArraySize,
            batchSavePubMsgCounter, batchSendMsgSize;

    private final BlockingQueue<MqttxOfflineMsg> pubMsgQueue;
    private final ScheduledExecutorService executorService;

    /**
     * listener执行次数 计数器
     */
    private AtomicInteger contentExecuteCounter = new AtomicInteger();

    public OfflineMsgDelayQueueExecutor(MqttxConfig mqttxConfig,
                                        ReactiveMongoTemplate mongoTemplate,
                                        IPublishMessageService publishMessageService) {
        super(mqttxConfig.getSysConfig().getPubAckTimeout(), ThreadPoolUtil.THREAD_NAME_OFFLINE_MSG_DELAY_QUEUE);

        this.mongoTemplate = mongoTemplate;
        this.publishMessageService = publishMessageService;

        MqttxConfig.SysConfig sysConfig = mqttxConfig.getSysConfig();
        this.batchSavePubMsgPoolSize = sysConfig.getBatchSaveMsgPoolSize();
        this.batchSavePubMsgQueueSize = sysConfig.getBatchSaveMsgQueueSize();
        this.batchSavePubMsgArraySize = sysConfig.getBatchSaveMsgArraySize();
        this.batchSavePubMsgCounter = sysConfig.getBatchSaveMsgCounter();
        this.batchSendMsgSize = sysConfig.getBatchSendMsgSize();

        pubMsgQueue = new LinkedBlockingDeque<>(batchSavePubMsgQueueSize);

        executorService = ThreadPoolUtil.newFixedScheduledExecutorService(
                batchSavePubMsgPoolSize, ThreadPoolUtil.THREAD_NAME_BATCH_SAVE_MSG);

        initBatchSaveMsgExecutor();
    }

    public void initBatchSaveMsgExecutor() {
        executorService.scheduleWithFixedDelay(() -> {
            try {
                if (pubMsgQueue.size() >= batchSavePubMsgArraySize) {
                    batchSave();
                } else {
                    int counter = contentExecuteCounter.incrementAndGet();
                    if (!pubMsgQueue.isEmpty() && counter >= batchSavePubMsgCounter) {
                        batchSave();
                        // 重新初始计数器
                        contentExecuteCounter = new AtomicInteger();
                    }
                }
            } catch (Exception e) {
                log.error("批量保存离线消息周期性线程池异常: {}", ExceptionUtil.getExceptionCause(e));
            }
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    @Scheduled(initialDelay = 10, fixedDelay = 10, timeUnit = TimeUnit.SECONDS)
    public void republishMsgScheduled() {
        mongoTemplate.findDistinct(MongoConstants.CLIENT_ID, MqttxOfflineMsg.class, String.class)
                .filter(clientId -> StringUtils.hasText(clientId) && ConnectHandler.CLIENT_MAP.containsKey(clientId))
                .collectList()
                .flatMapMany(clientIds -> {
                    if (CollectionUtils.isEmpty(clientIds)) {
                        return Flux.empty();
                    }
                    return mongoTemplate.find(
                            Query.query(Criteria.where(MongoConstants.CLIENT_ID).in(clientIds))
                                    .with(Sort.by(Sort.Direction.ASC, MongoConstants.RETRY_TIME))
                                    .limit(batchSendMsgSize), MqttxOfflineMsg.class);
                })
                .publishOn(Schedulers.boundedElastic())
                .map(msg -> {
                    publishMessageService.republish(PubMsg.of(msg)).subscribe();
                    return msg;
                })
                .collectList()
                .doOnSuccess(msg -> {
                    List<Integer> messageIds = msg.stream().map(MqttxOfflineMsg::getMessageId).toList();
                    mongoTemplate.updateMulti(
                            Query.query(Criteria.where(MongoConstants.CLIENT_ID).in(messageIds)),
                            Update.update(MongoConstants.RETRY_TIME, LocalDateTime.now()), MqttxOfflineMsg.class)
                            .subscribe();
                })
                .subscribe();
    }

    @Override
    public void invoke(OfflineMsgDelay offlineMsgDelay) {
        String clientId = offlineMsgDelay.getClientId();
        int messageId = offlineMsgDelay.getMessageId();
        PubMsg pubMsg = publishMessageService.removeCache(clientId, messageId);
        if (Objects.isNull(pubMsg)) {
            return;
        }

        addBlockingQueue(pubMsg, offlineMsgDelay);
    }

    /**
     * 批量保存发布消息
     */
    public void batchSave() {
        long start = System.currentTimeMillis();
        List<MqttxOfflineMsg> list = new ArrayList<>();
        pubMsgQueue.drainTo(list, batchSavePubMsgArraySize);

        Flux.fromIterable(list)
                .flatMap(msg -> mongoTemplate.upsert(
                        Query.query(Criteria.where(MongoConstants.CLIENT_ID).is(msg.getClientId())
                                .and(MongoConstants.MESSAGE_ID).is(msg.getMessageId())),
                        Update.update(MongoConstants.RECORD_TIME, LocalDateTime.now())
                                .set(MongoConstants.QOS, msg.getQos())
                                .set(MongoConstants.PAYLOAD, msg.getPayload())
                                .set(MongoConstants.TOPIC, msg.getTopic())
                                .setOnInsert(MongoConstants.MESSAGE_ID, msg.getMessageId())
                                .setOnInsert(MongoConstants.CLIENT_ID, msg.getClientId()),
                        MqttxOfflineMsg.class))
                .collectList()
                .doOnSuccess(result ->
                        log.info("批量保存离线消息: 消息数量[{}], 成功数量[{}], 耗时[{}]毫秒",
                                list.size(), result.size(), System.currentTimeMillis() - start))
                .subscribe();
    }

    /**
     * 将消息添加到队列中
     *
     * @param pubMsg
     */
    public void addBlockingQueue(PubMsg pubMsg, OfflineMsgDelay offlineMsgDelay) {
        try {
            // 队列容量不足时，重新加入到本地延时队列中
            if (pubMsgQueue.size() >= batchSavePubMsgArraySize) {
                // 抑制日志打印，防止日志打印太多
                long now = System.currentTimeMillis();
                if (now - lastLogTime.get() < MAX_LOG_INTERVAL_TIME) {
                    lastLogTime.set(now);
                    log.warn("离线消息队列容量超过阈值!");
                }
                addQueue(offlineMsgDelay);
                return;
            }

            pubMsgQueue.put(MqttxOfflineMsg.of(pubMsg));
        } catch (Exception e) {
            log.error("添加离线消息到阻塞队列异常 = {}", ExceptionUtil.getAllExceptionStackTrace(e));
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
