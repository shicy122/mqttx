package com.hycan.idn.mqttx.controller;

import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.broker.handler.PublishHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.controller.pojo.MqttPubMsgReq;
import com.hycan.idn.mqttx.pojo.ClientConnOrDiscMsg;
import com.hycan.idn.mqttx.pojo.InternalMessage;
import com.hycan.idn.mqttx.pojo.PubMsg;
import com.hycan.idn.mqttx.service.IAuthenticationService;
import com.hycan.idn.mqttx.service.IInternalMessageService;
import com.hycan.idn.mqttx.utils.BytesUtil;
import com.hycan.idn.mqttx.utils.IPUtils;
import com.hycan.idn.mqttx.utils.SHA256Util;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import lombok.extern.slf4j.Slf4j;
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
import java.util.List;
import java.util.Objects;

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

    private final StringRedisTemplate redisTemplate;

    private final String adminPwdStrPrefix, brokerId, instanceId, DISCONNECT;

    private final int innerPwdStrPrefixTimeout;

    private final boolean isCluster;

    /** 内部消息发布服务 */
    private final IInternalMessageService internalMessageService;

    /** 认证服务 */
    private final IAuthenticationService authenticationService;

    public MqttController(StringRedisTemplate redisTemplate,
                          MqttxConfig mqttxConfig, PublishHandler publishHandler,
                          IInternalMessageService internalMessageService,
                          IAuthenticationService authenticationService) {
        this.redisTemplate = redisTemplate;
        this.publishHandler = publishHandler;

        MqttxConfig.Redis redis = mqttxConfig.getRedis();
        MqttxConfig.Cluster cluster = mqttxConfig.getCluster();

        this.brokerId = mqttxConfig.getBrokerId();
        this.instanceId = mqttxConfig.getInstanceId();
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
    public ResponseEntity<String> publish(@RequestBody @Valid MqttPubMsgReq mqttMsg) {
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
            if (!ConnectHandler.CLIENT_MAP.containsKey(clientId) && isCluster) {
                internalMessageService.publish(
                        new InternalMessage<>(ClientConnOrDiscMsg.offline(clientId, instanceId),
                                System.currentTimeMillis(), brokerId), DISCONNECT);
            } else {
                ChannelId channelId = ConnectHandler.CLIENT_MAP.get(clientId);
                if (Objects.isNull(channelId)) {
                    continue;
                }
                Channel channel = BrokerHandler.CHANNELS.find(channelId);
                if (Objects.nonNull(channel) && channel.isActive()) {
                    // 关闭连接
                    channel.close();
                }
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
}
