package com.hycan.idn.mqttx.constants;

import java.time.ZoneOffset;

/**
 * 时间相关常量
 *
 * @author Shadow
 * @since 1.2.1
 */
public interface TimeConstants {

    String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    String DATE_FORMAT = "yyyy-MM-dd";

    String TIME_FORMAT = "HH:mm:ss";

    /** 时区-北京时间 */
    ZoneOffset BEI_JING = ZoneOffset.of("+8");
}
