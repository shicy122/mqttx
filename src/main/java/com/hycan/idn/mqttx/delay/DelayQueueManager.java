package com.hycan.idn.mqttx.delay;

import com.hycan.idn.mqttx.utils.ExceptionUtil;
import com.hycan.idn.mqttx.utils.ThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

/**
 * 初始化队列监听
 *
 * @author shichongying
 */
@Slf4j
public abstract class DelayQueueManager<T> {

    public final DelayQueue<DelayQueueElement<T>> delayQueue = new DelayQueue<>();

    private final int pubAckTimeout;

    protected DelayQueueManager(int pubAckTimeout, String threadName) {
        this.pubAckTimeout = pubAckTimeout;

        startThread(threadName);
    }

    /**
     * 启动线程获取队列
     */
    @SuppressWarnings({"InfiniteLoopStatement"})
    public void startThread(String threadName) {
        ThreadPoolUtil.newSingleThreadPoolExecutor(threadName).execute(() -> {
            while (true) {
                try {
                    DelayQueueElement<T> element = delayQueue.take();
                    if (Objects.isNull(element.getT())) {
                        continue;
                    }
                    invoke(element.getT());
                } catch (Exception e) {
                    log.error("延时队列线程错误 = {}", ExceptionUtil.getExceptionCause(e));
                    try {
                        TimeUnit.MICROSECONDS.sleep(10);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
    }

    /**
     * 向延迟队列中添加 value 抽象方法
     *
     * @param t
     */
    public void addQueue(T t) {
        DelayQueueElement<T> element = new DelayQueueElement<>(t, pubAckTimeout, TimeUnit.SECONDS);
        delayQueue.put(element);
    }

    /**
     * 延迟队列到期回调抽象方法，子类按业务实现逻辑
     *
     * @param t
     */
    public abstract void invoke(T t);
}