package com.hycan.idn.mqttx.exception;

/**
 * json 相关异常
 *
 * @author Shadow
 * @since 1.0.0
 */
public class JsonException extends GlobalException {

    public JsonException() {
        super();
    }

    public JsonException(String message) {
        super(message);
    }

    public JsonException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonException(Throwable cause) {
        super(cause);
    }
}
