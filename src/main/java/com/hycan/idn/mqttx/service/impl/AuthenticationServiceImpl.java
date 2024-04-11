package com.hycan.idn.mqttx.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.broker.handler.AbstractMqttSessionHandler;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.constants.MongoConstants;
import com.hycan.idn.mqttx.entity.MqttxUser;
import com.hycan.idn.mqttx.exception.AuthenticationException;
import com.hycan.idn.mqttx.pojo.Authentication;
import com.hycan.idn.mqttx.pojo.ClientAuth;
import com.hycan.idn.mqttx.service.IAuthenticationService;
import com.hycan.idn.mqttx.service.IInstanceRouterService;
import com.hycan.idn.mqttx.utils.ExceptionUtil;
import com.hycan.idn.mqttx.utils.JSON;
import com.hycan.idn.mqttx.utils.R;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

/**
 * 认证服务, 提供最基础的认证服务.
 * <p>
 * 当用户指定了配置项 mqttx.auth.url, 该对象使用 {@link HttpClient} 发出 POST 请求给 mqttx.auth.url。
 * 注意：接口返回 http status = 200 即表明认证成功, 其它状态值一律为认证失败
 * </p>
 *
 * @author Shadow
 * @since 2.0.1
 */
@Slf4j
@Service
public class AuthenticationServiceImpl implements IAuthenticationService {

    private static final String HTTPS = "https";
    private static final String HTTP = "http";

    private final HttpClient httpClient;
    private final ReactiveMongoTemplate mongoTemplate;
    private final ReactiveStringRedisTemplate stringRedisTemplate;
    private final IInstanceRouterService instanceRouterService;
    private final String adminKeyStrPrefix, adminConnHashPrefix, instanceId;
    private final int innerPwdStrPrefixTimeout;
    private final List<String> adminClientIdPrefix;
    private final Map<String, String> endpointMap;
    private final Duration timeout;
    private final boolean enableLog;

    /**
     * 创建一个新的实例
     *
     * @param config                配置项
     * @param httpClient            {@link HttpClient}
     *                              //@param redisTemplate
     * @param mongoTemplate
     * @param stringRedisTemplate
     * @param instanceRouterService
     */
    public AuthenticationServiceImpl(MqttxConfig config,
                                     HttpClient httpClient,
                                     ReactiveMongoTemplate mongoTemplate,
                                     ReactiveStringRedisTemplate stringRedisTemplate,
                                     IInstanceRouterService instanceRouterService) {
        this.mongoTemplate = mongoTemplate;
        this.stringRedisTemplate = stringRedisTemplate;
        this.instanceRouterService = instanceRouterService;

        MqttxConfig.Redis redis = config.getRedis();
        this.adminKeyStrPrefix = redis.getAdminKeyStrPrefix();
        this.adminConnHashPrefix = redis.getAdminConnHashPrefix();
        this.innerPwdStrPrefixTimeout = redis.getInnerPwdStrPrefixTimeout();
        this.adminClientIdPrefix = config.getAuth().getAdminClientIdPrefix();
        this.endpointMap = config.getAuth().getEndpointMap();
        this.enableLog = config.getSysConfig().getEnableLog();
        this.instanceId = config.getInstanceId();

        MqttxConfig.Auth auth = config.getAuth();
        timeout = auth.getTimeout();

        if (Boolean.TRUE.equals(auth.getEnable())) {
            if (CollectionUtils.isEmpty(endpointMap)) {
                throw new IllegalArgumentException("MQTTX已开启客户端鉴权，Endpoint配置不能为空");
            }

            for (String endpoint : endpointMap.values()) {
                URI uri = URI.create(endpoint);
                String scheme = uri.getScheme();
                if (scheme == null) {
                    throw new IllegalArgumentException(format("mqttx.auth.endpoint [%s] 的 scheme 为空", endpoint));
                }
                scheme = scheme.toLowerCase(Locale.US);
                if (!(scheme.equals(HTTPS) || scheme.equals(HTTP))) {
                    throw new IllegalArgumentException(format("mqttx.auth.endpoint [%s] 的 scheme 无效", uri));
                }
                if (uri.getHost() == null) {
                    throw new IllegalArgumentException(format("不支持的 mqttx.auth.endpoint [%s]", uri));
                }
            }
        }
        this.httpClient = httpClient;
    }

    @Override
    public CompletableFuture<Authentication> asyncAuthenticate(ClientAuth authDTO) {
        if (Boolean.TRUE.equals(enableLog)) {
            log.info("开始鉴权: 客户端ID=[{}], 用户名=[{}], 密码=[{}]", authDTO.getClientId(), authDTO.getUsername(), authDTO.getPassword());
        }
        String url = getUrl(authDTO.getClientId());
        if (!StringUtils.hasText(url)) {
            Query query = Query.query(Criteria.where(MongoConstants.USER_NAME).is(authDTO.getUsername()));
            mongoTemplate.findOne(query, MqttxUser.class)
                    .doOnSuccess(user -> {
                        if (Objects.isNull(user) || !authDTO.getPassword().equals(user.getPassword())) {
                            throw new AuthenticationException(format("鉴权失败: 用户名或密码错误, 客户端ID=[%s]", authDTO.getClientId()));
                        }
                    })
                    .doOnError(t -> log.error(t.getMessage(), t))
                    .block();
            return CompletableFuture.completedFuture(Authentication.of(authDTO.getClientId(),
                    Collections.singletonList(TopicUtils.ALL_TOPIC),
                    Collections.singletonList(TopicUtils.ALL_TOPIC)));
        }

        var httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(timeout)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(authDTO)))
                .build();
        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenApply(resp -> {
                    if (!HttpStatus.valueOf(resp.statusCode()).is2xxSuccessful()) {
                        throw new AuthenticationException(
                                format("鉴权失败: 用户名=[%s], HTTP状态码[%s], 原因:[%s]",
                                        authDTO.getUsername(), resp.statusCode(), resp.body()));
                    }
                    return resp.body();
                })
                .thenApply(body -> {
                    R<Authentication> result = JSON.readValue(body, new TypeReference<>() {});
                    return result.getData();
                });
    }

    private String getUrl(String clientId) {
        Optional<String> urlOptional = endpointMap.entrySet().stream()
                .filter(entry -> clientId.startsWith(entry.getKey()))
                .map(Map.Entry::getValue)
                .findAny();

        return urlOptional.orElse(null);
    }

    /**
     * 修改用户授权的 pub&sub topic 列表
     *
     * @param clientId            客户端 ID
     * @param authorizedSubTopics 被授权订阅 topic 列表
     * @param authorizedPubTopics 被授权发布 topic 列表
     */
    @Override
    public void alterUserAuthorizedTopic(String clientId, List<String> authorizedSubTopics, List<String> authorizedPubTopics) {
        if (CollectionUtils.isEmpty(authorizedPubTopics) && CollectionUtils.isEmpty(authorizedSubTopics)) {
            return;
        }

        Optional.ofNullable(ConnectHandler.CLIENT_MAP.get(clientId))
                .map(BrokerHandler.CHANNELS::find)
                .ifPresent(channel -> {
                    if (!CollectionUtils.isEmpty(authorizedPubTopics)) {
                        channel.attr(AttributeKey.valueOf(AbstractMqttSessionHandler.AUTHORIZED_PUB_TOPICS)).set(authorizedPubTopics);
                    }
                    if (!CollectionUtils.isEmpty(authorizedSubTopics)) {
                        channel.attr(AttributeKey.valueOf(AbstractMqttSessionHandler.AUTHORIZED_SUB_TOPICS)).set(authorizedSubTopics);
                    }
                });
    }

    /**
     * 管理员客户端对应的密码续期（PUBLISH、PUBACK、PINGREQ、系统上下线事件）
     *
     * @param clientId 管理员客户端ID
     */
    @Override
    public void incAdminClientExpireTime(String clientId) {
        try {
            boolean isExist = adminClientIdPrefix.stream().anyMatch(clientId::startsWith);
            if (isExist) {
                // 不校验是否存在，不存在则说明客户端信息非法，不对密码续期
                stringRedisTemplate.expire(adminKeyStrPrefix + clientId, Duration.ofHours(innerPwdStrPrefixTimeout)).subscribe();
            }
        } catch (Exception e) {
            log.error("管理员客户端密码续期异常 = {}", ExceptionUtil.getExceptionCause(e));
        }
    }

    @Override
    public Mono<MqttConnectReturnCode> adminAuthenticate(ClientAuth authDTO) {
        return instanceRouterService.adminClientConnectRouter(authDTO.getClientId())
                .flatMap(result -> {
                    if (Boolean.TRUE.equals(result)) {
                        return validAdminClientAuth(authDTO);
                    }
                    log.warn("拒绝连接: 管理员客户端ID=[{}]需要负载均衡到其他MQTTX节点!", authDTO.getClientId());
                    return Mono.just(MqttConnectReturnCode.CONNECTION_REFUSED_USE_ANOTHER_SERVER);
                });
    }

    /**
     * 校验admin客户端鉴权信息
     *
     * @return true/false
     */
    private Mono<MqttConnectReturnCode> validAdminClientAuth(ClientAuth authDTO) {
        return stringRedisTemplate.opsForValue().get(adminKeyStrPrefix + authDTO.getClientId())
                .flatMap(pwd -> {
                    if (Objects.equals(pwd, authDTO.getPassword())) {
                        return stringRedisTemplate.opsForHash()
                                .put(adminConnHashPrefix, authDTO.getClientId(), instanceId)
                                .then(Mono.just(MqttConnectReturnCode.CONNECTION_ACCEPTED));
                    }
                    log.warn("鉴权失败: 客户端ID=[{}], 用户名=[{}], 密码=[{}]",
                            authDTO.getClientId(), authDTO.getUsername(), authDTO.getPassword());
                    return Mono.just(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                });
    }
}