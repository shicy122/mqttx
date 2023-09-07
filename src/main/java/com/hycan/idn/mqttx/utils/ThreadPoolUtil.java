package com.hycan.idn.mqttx.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类
 *
 * @author shichongying
 */
@Slf4j
public class ThreadPoolUtil {

    public static final int DEFAULT_CAPACITY = 10000;

    private static final Thread.UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER = (t, e) ->
            log.error("thread {} encounter a uncaught exception: {}", t.getName(), e.toString());

    public static final String THREAD_NAME_BATCH_SAVE_MSG = "mqttx_batch_save_msg";

    public static final String THREAD_NAME_PUB_SYS_STATE = "mqttx_pub_sys_state";

    public static final String THREAD_NAME_LOCAL_MSG_DELAY_QUEUE = "mqttx_local_msg_delay_queue";

    public static final String THREAD_NAME_OFFLINE_MSG_DELAY_QUEUE = "mqttx_offline_msg_delay_queue";

    public static final String THREAD_NAME_SEND_BRIDGE_MSG = "mqttx_send_bridge_msg";

    public static ExecutorService newSingleThreadPoolExecutor(String threadName) {
        return newThreadPoolExecutor(1, 1, threadName, null);
    }

    public static ExecutorService newThreadPoolExecutor(int corePoolSize, String threadName) {
        return newThreadPoolExecutor(corePoolSize, corePoolSize * 2, threadName, null);
    }

    public static ExecutorService newThreadPoolExecutor(int corePoolSize,
                                                        int maxPoolSize,
                                                        String threadName,
                                                        RejectedExecutionHandler handler) {
        return newThreadPoolExecutor(corePoolSize, maxPoolSize, DEFAULT_CAPACITY, threadName, handler);
    }

    public static ExecutorService newThreadPoolExecutor(int corePoolSize, int maxPoolSize, int capacity,
                                                        String threadName, RejectedExecutionHandler handler) {
        ThreadFactory factory = createThreadFactory(threadName);
        if (corePoolSize < 0) {
            corePoolSize = 1;
        }

        if (maxPoolSize < 0) {
            maxPoolSize = 1;
        }

        if (capacity < 0) {
            capacity = DEFAULT_CAPACITY;
        }

        if (Objects.isNull(handler)) {
            handler = new ThreadPoolExecutor.CallerRunsPolicy();
        }

        return new ThreadPoolExecutor(corePoolSize, maxPoolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(capacity), factory, handler);
    }

    public static ScheduledExecutorService newFixedScheduledExecutorService(int poolSize, String threadName) {
        ThreadFactory factory = Executors.defaultThreadFactory();
        if (!ObjectUtils.isEmpty(threadName)) {
            factory = createThreadFactory(threadName);
        }
        if (poolSize < 0) {
            poolSize = 1;
        }
        return new ScheduledThreadPoolExecutor(poolSize, factory);
    }

    /**
     * 创建一个线程工程，产生的线程格式为：Thread-threadName-1, Thread-threadName-2
     *
     * @param threadName 线程名
     * @return 线程工程，用于线程池创建可读的线程
     */
    private static ThreadFactory createThreadFactory(String threadName) {
        return new ThreadFactoryBuilder()
                .setNameFormat("Thread-" + threadName + "-%d")
                .setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER)
                .setDaemon(false)
                .build();
    }
}
