package com.hycan.idn.mqttx.constants;

/**
 * MongoDB操作相关常量
 * @author shadow
 */
public interface MongoConstants {
    //@formatter:off

    /** 用户名 */
    String USER_NAME = "username";

    String PASSWORD = "password";

    String SALT = "salt";

    String IS_SUPER_USER = "is_super_user";

    /** 客户端ID **/
    String CLIENT_ID = "client_id";

    /** 消息ID **/
    String MESSAGE_ID = "message_id";

    /** 消息主题 **/
    String TOPIC = "topic";

    String QOS = "qos";

    String PAYLOAD = "payload";

    String VERSION = "version";

    String CLEAN_SESSION = "clean_session";

    String RECORD_TIME = "record_time";

    String RETRY_TIME = "retry_time";

    //@formatter:on
}
