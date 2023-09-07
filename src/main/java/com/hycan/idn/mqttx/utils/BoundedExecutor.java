package com.hycan.idn.mqttx.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 使用Semaphore来控制任务的提交速率
 *
 * @author shichongying
 */
@Slf4j
public class BoundedExecutor {
    private final Executor exec;
    private final Semaphore semaphore;
    int bound;

    public BoundedExecutor(Executor exec, int bound) {
        this.exec = exec;
        this.semaphore = new Semaphore(bound);
        this.bound = bound;
    }

    public void submitTask(final Runnable command) {
        try {
            // 通过 acquire() 获取一个许可
            semaphore.acquire();
        } catch (InterruptedException e) {
            log.error("获取信号量许可异常 = {}", ExceptionUtil.getExceptionCause(e));
            try {
                TimeUnit.MICROSECONDS.sleep(10);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        try {
            exec.execute(() -> {
                try {
                    command.run();
                } finally {
                    // release() 释放一个许可
                    semaphore.release();
                }
            });
        } catch (RejectedExecutionException e) {
            log.error("execute thread throw exception={}", ExceptionUtil.getAllExceptionStackTrace(e));
            semaphore.release();
        }
    }
}
