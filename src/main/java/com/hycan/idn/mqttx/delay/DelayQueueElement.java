package com.hycan.idn.mqttx.delay;

import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author shichongying
 * @datetime 2023年 02月 01日 17:15
 */
@Data
public class DelayQueueElement<T> implements Delayed {

    private final T t;
    private final long expireTime;

    public DelayQueueElement(T t, long delay, TimeUnit unit) {
        this.t = t;
        this.expireTime = System.nanoTime() + unit.toNanos(delay);
    }

    /**
     * Delayed接口的抽象方法 定义当前距离目标时间的延迟时间
     *
     * @param unit
     * @return
     */
    @Override
    public long getDelay(TimeUnit unit) {
        // 执行目标时间 - 当前时间
        return unit.convert(expireTime - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    /**
     * Comparable接口的抽象方法 比较两个Delayed对象的大小 会用于确定任务在优先级队列中的排序 使用Delayed#getDelay来计算
     *
     * @param delayed 延迟对象
     * @return
     */
    @Override
    public int compareTo(@NotNull Delayed delayed) {
        DelayQueueElement other = (DelayQueueElement) delayed;
        long diff = expireTime - other.expireTime;
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        } else {
            return 0;
        }
    }
}
