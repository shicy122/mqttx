package com.hycan.idn.mqttx.delay.exec;

import com.hycan.idn.mqttx.broker.handler.PublishHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.delay.DelayQueueManager;
import com.hycan.idn.mqttx.delay.pojo.LocalMsgDelay;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.pojo.Session;
import com.hycan.idn.mqttx.service.IPublishMessageService;
import com.hycan.idn.mqttx.utils.ThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Map;

/**
 * 发布消息本地延迟队列
 *
 * @author shichongying
 * @datetime 2023年 02月 07日 20:20
 */
@Slf4j
@Component
public class LocalMsgDelayQueueExecutor extends DelayQueueManager<LocalMsgDelay> {

    private final IPublishMessageService publishMessageService;

    public LocalMsgDelayQueueExecutor(MqttxConfig config, IPublishMessageService publishMessageService) {
        super(config.getSysConfig().getPubAckTimeout(), ThreadPoolUtil.THREAD_NAME_LOCAL_MSG_DELAY_QUEUE);

        this.publishMessageService = publishMessageService;
    }

    @Override
    public void invoke(LocalMsgDelay localMsgDelay) {
        Session session = localMsgDelay.getSession();
        int messageId = localMsgDelay.getMessageId();
        Map<Integer, PubMsg> pubMsgStore = session.getPubMsgStore();
        if (CollectionUtils.isEmpty(pubMsgStore) || !pubMsgStore.containsKey(messageId)) {
            return;
        }

        // 超时时间内未响应PUB_ACK，则重发消息
        PubMsg pubMsg = pubMsgStore.get(messageId);
        publishMessageService.republish(pubMsg).subscribe();
    }
}
