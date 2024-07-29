package com.hycan.idn.mqttx.controller;

import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.broker.handler.PublishHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.constants.MongoConstants;
import com.hycan.idn.mqttx.controller.pojo.MqttPubMsgReqDTO;
import com.hycan.idn.mqttx.entity.MqttxRetainMsg;
import com.hycan.idn.mqttx.entity.MqttxUser;
import com.hycan.idn.mqttx.pojo.ClientConnOrDiscMsg;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.pojo.UserInfo;
import com.hycan.idn.mqttx.service.IAuthenticationService;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.utils.BytesUtil;
import com.hycan.idn.mqttx.utils.SHA256Util;
import io.netty.channel.ChannelOutboundInvoker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * MQTT 相关接口
 *
 * @author Shadow
 */
@Slf4j
@Validated
@RestController
@RequestMapping("/api/v1/mqtt")
public class MqttController {

    private final PublishHandler publishHandler;

    private final ReactiveMongoTemplate mongoTemplate;

    private final StringRedisTemplate redisTemplate;

    private final String adminPwdStrPrefix, brokerId, currentInstanceId, DISCONNECT;

    private final int innerPwdStrPrefixTimeout;

    private final boolean isCluster;

    /**
     * 内部消息发布服务
     */
    private final IInternalMessageService internalMessageService;

    /**
     * 认证服务
     */
    private final IAuthenticationService authenticationService;

    public MqttController(StringRedisTemplate redisTemplate,
                          MqttxConfig mqttxConfig, PublishHandler publishHandler,
                          ReactiveMongoTemplate mongoTemplate,
                          IInternalMessageService internalMessageService,
                          IAuthenticationService authenticationService) {
        this.redisTemplate = redisTemplate;
        this.publishHandler = publishHandler;
        this.mongoTemplate = mongoTemplate;

        MqttxConfig.Redis redis = mqttxConfig.getRedis();
        MqttxConfig.Cluster cluster = mqttxConfig.getCluster();

        this.brokerId = mqttxConfig.getBrokerId();
        this.currentInstanceId = mqttxConfig.getInstanceId();
        this.isCluster = cluster.getEnable();
        this.DISCONNECT = mqttxConfig.getKafka().getDisconnect();

        this.adminPwdStrPrefix = redis.getAdminKeyStrPrefix();
        this.innerPwdStrPrefixTimeout = redis.getInnerPwdStrPrefixTimeout();
        this.internalMessageService = internalMessageService;
        this.authenticationService = authenticationService;
    }

    /**
     * 发布消息
     *
     * @param mqttMsg 请求结构体
     * @return Void
     */
    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestBody @Valid MqttPubMsgReqDTO mqttMsg) {
        @NotNull byte[] payload = mqttMsg.getPayload();
        String topic = mqttMsg.getTopic();
        int qos = mqttMsg.getQos();
        log.info("收到下行消息请求: Topic=[{}], Payload=[{}]", topic, BytesUtil.bytesToHexString(payload));
        final var pubMsg = PubMsg.of(qos, topic, payload);
        try {
            publishHandler.publish(pubMsg, null, false).subscribe();
            return ResponseEntity.ok("OK");
        } catch (Exception e) {
            log.error("消息发送异常，exception={}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("服务端处理异常");
        }
    }

    /**
     * 客户端断线
     *
     * @param clientIds 客户端ID列表
     * @return Void
     */
    @PostMapping("/disconnect")
    public ResponseEntity<String> disconnect(@RequestBody @Valid @Size(min = 1, max = 100) List<String> clientIds) {
        log.info("收到断开连接请求: 客户端列表={}", clientIds);
        for (String clientId : clientIds) {
            if (ConnectHandler.CLIENT_MAP.containsKey(clientId)) {
                Optional.ofNullable(ConnectHandler.CLIENT_MAP.get(clientId))
                        .map(BrokerHandler.CHANNELS::find)
                        .ifPresent(ChannelOutboundInvoker::close);
            } else if (isCluster) {
                internalMessageService.publish(
                        new InternalMessage<>(ClientConnOrDiscMsg.offline(clientId, currentInstanceId),
                                System.currentTimeMillis(), brokerId), DISCONNECT);
            }
        }

        return ResponseEntity.ok("OK");
    }

    /**
     * 获取客户端动态密码
     *
     * @param clientId 客户端ID
     * @return Void
     */
    @GetMapping("/encrypt/{client_id}")
    public ResponseEntity<String> encrypt(@NotBlank @PathVariable("client_id") String clientId) {
        log.info("收到获取秘钥请求: 客户端ID=[{}]", clientId);
        String pwd = redisTemplate.opsForValue().get(adminPwdStrPrefix + clientId);
        if (StringUtils.hasText(pwd)) {
            authenticationService.incAdminClientExpireTime(clientId);
        } else {
            pwd = SHA256Util.signWithHmacSha256(String.valueOf(System.nanoTime()), clientId);
            redisTemplate.opsForValue().set(adminPwdStrPrefix + clientId, pwd, Duration.ofHours(innerPwdStrPrefixTimeout));
        }
        return ResponseEntity.ok(pwd);
    }

    /**
     * 添加用户信息
     *
     * @param userInfo 用户信息
     * @return Void
     */
    @PostMapping("/user")
    public ResponseEntity<String> createUser(@RequestBody @Valid UserInfo userInfo) {
        log.info("收到创建用户请求: 用户名=[{}], 密码=[{}]", userInfo.getUsername(), anonymizeValue(userInfo.getPassword()));
        Query query = Query.query(Criteria.where(MongoConstants.USER_NAME).is(userInfo.getUsername()));
        Update update = Update.update(MongoConstants.RECORD_TIME, LocalDateTime.now())
                .set(MongoConstants.IS_SUPER_USER, 0)
                .set(MongoConstants.PASSWORD, userInfo.getPassword())
                .setOnInsert(MongoConstants.USER_NAME, userInfo.getUsername());
        mongoTemplate.upsert(query, update, MqttxUser.class)
                .doOnError(t -> log.error(t.getMessage(), t))
                .block();

        return ResponseEntity.ok("OK");
    }

    private String anonymizeValue(String fieldValue) {
        String value = fieldValue.replaceAll("\"", "");
        String anonymizedValue;
        if (value.length() >= 12) {
            anonymizedValue = "\"" + value.substring(0, 4) + "****" + value.substring(value.length() - 4) + "\"";
        } else if (value.length() >= 8) {
            anonymizedValue = "\"" + value.substring(0, 2) + "****" + value.substring(value.length() - 2) + "\"";
        } else {
            anonymizedValue = "\"****\"";
        }
        return anonymizedValue;
    }
}
