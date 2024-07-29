/*
 * Copyright 2002-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hycan.idn.mqttx.config;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hycan.idn.mqttx.broker.handler.PublishHandler;
import com.hycan.idn.mqttx.constants.SerializeStrategy;
import com.hycan.idn.mqttx.constants.ShareStrategyEnum;
import com.hycan.idn.mqttx.pojo.TopicRateLimit;
import com.hycan.idn.mqttx.utils.IPUtils;
import com.hycan.idn.mqttx.utils.TopicUtils;
import io.netty.handler.codec.mqtt.MqttConstant;
import io.netty.handler.ssl.ClientAuth;
import lombok.Data;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.*;

/**
 * 业务配置.
 * <p>
 * 所有项目都有默认配置, 其它类注入配置后不再检查配置是否为空
 *
 * @author Shadow
 * @since 2.0.1
 */
@Data
@Component
@ConfigurationProperties(prefix = "mqttx")
public class MqttxConfig {

    @JsonIgnore
    @Resource
    private NacosDiscoveryProperties nacosDiscoveryProperties;

    @JsonIgnore
    @Resource
    private ServerProperties serverProperties;

    //@formatter:off

    /*--------------------------------------------
    |                 基础配置项                   |
    ============================================*/

    /** mqttx 版本 */
    private String version = "unknown";

    /** broker id。区分集群内不同的 broker（如果集群功能开启）。默认对UUID hash后取模 */
    private String brokerId = IPUtils.getHostName();

    private String instanceId;

    /** 心跳, 默认 60S; 如果 客户端通过 conn 设置了心跳周期，则对应的 channel 心跳为指定周期 */
    private Duration heartbeat = Duration.ofMinutes(1);

    /** keepalive 系数, 默认 1.5 倍, 支持的范围为 (0, 3] */
    private Double keepaliveRatio = 1.5D;

    /** ip */
    private String host = "0.0.0.0";

    /** tcp连接队列 */
    private Integer soBacklog = 512;

    /**
     * 序列化策略选择:
     * <ol>
     *     <li>{@link SerializeStrategy#JSON}: 默认项(兼容早期版本)</li>
     *     <li>{@link SerializeStrategy#KRYO}: 优选项，性能领先 json</li>
     * </ol>
     */
    private String serializeStrategy = SerializeStrategy.JSON;

    /**
     * 有时候 client 会发表消息给自己（client 订阅了自己发布的主题），默认情形 mqttx 将过滤该消息.
     */
    private Boolean ignoreClientSelfPub = true;

    /** {@link java.net.http.HttpClient} connectTimeout */
    private Duration httpClientConnectTimeout = Duration.ofSeconds(10);

    /** mqttx 可接受的最大报文大小 */
    private int maxBytesInMessage = MqttConstant.DEFAULT_MAX_BYTES_IN_MESSAGE;

    /** mqttx 可接受的最大clientId长度 */
    private int maxClientIdLength = MqttConstant.DEFAULT_MAX_CLIENT_ID_LENGTH;

    public String getInstanceId() {
        instanceId = IPUtils.getHostIp() + "#" +
                serverProperties.getPort() + "#" +
                nacosDiscoveryProperties.getClusterName() + "#" +
                nacosDiscoveryProperties.getGroup() + "@@" +
                nacosDiscoveryProperties.getService();
        return instanceId;
    }

    /*--------------------------------------------
    |                 模块配置项                   |
    ============================================*/

    private Redis redis = new Redis();

    private Kafka kafka = new Kafka();

    private Cluster cluster = new Cluster();

    private Ssl ssl = new Ssl();

    private Socket socket = new Socket();

    private WebSocket webSocket = new WebSocket();

    private FilterTopic filterTopic = new FilterTopic();

    private SysConfig sysConfig = new SysConfig();

    private ShareTopic shareTopic = new ShareTopic();

    private SysTopic sysTopic = new SysTopic();

    private MessageBridge messageBridge = new MessageBridge();

    private RateLimiter rateLimiter = new RateLimiter();

    private Auth auth = new Auth();

    /**
     * redis 配置
     *
     * 目前 mqttx 的储存、集群功能的默认实现都依赖 redis，耦合过重不利于其他实现（如 mysql/kafka），先抽出配置项.
     * ps: 实际上集群功能的实现也是基于 redis
     */
    @Data
    public static class Redis {

        /** client pubRel 消息 redis set 前缀 */
        private String pubRelMsgSetPrefix = "mqttx:client:msg:pubrel:";

        /** 非 cleanSession client messageId 前缀 */
        private String messageIdIncPrefix = "mqttx:client:messageid:";

        /** 内部 client 账号临时加密秘钥前缀 */
        private String adminKeyStrPrefix = "mqttx:client:admin:key:";

        /** 内部 client 账号临时加密密码前缀 */
        private String adminConnHashPrefix = "mqttx:client:admin:conn";

        /** 内部 client 账号加密秘密码过期时间，默认 24 小时 */
        private Integer innerPwdStrPrefixTimeout = 24;
    }


    @Data
    public static class Kafka {

        private String groupIdPrefix = "mqttx";

        private Integer concurrency = 11;

        private String sync = "mqttx-internal-sync";

        private String pub = "mqttx-internal-pub";

        private String pubAck = "mqttx-internal-puback";

        private String pubRec = "mqttx-internal-pubrec";

        private String pubCom = "mqttx-internal-pubcom";

        private String pubRel = "mqttx-internal-pubrel";

        private String connect = "mqttx-internal-connect";

        private String disconnect = "mqttx-internal-disconnect";

        private String authorized = "mqttx-internal-authorized";

        private String subOrUnsub = "mqttx-internal-suborunsub";

        private String sys = "mqttx-internal-sys";
    }

    /**
     * 集群配置
     */
    @Data
    public static class Cluster {

        /** 集群开关 */
        private Boolean enable = false;
    }

    /**
     * ssl 配置
     */
    @Data
    public static class Ssl {

        /** ssl 开关 */
        private Boolean enable = false;

        /** keyStore 位置 */
        private String keyStoreLocation = "classpath:tls/mqttx.keystore";

        /** keyStore 密码 */
        private String keyStorePassword = "123456";

        /** keyStore 类别 */
        private String keyStoreType = "pkcs12";

        /** 客户端认证 */
        private ClientAuth clientAuth = ClientAuth.NONE;
    }

    /**
     * socket 配置
     */
    @Data
    public static class Socket {

        /** 开关 */
        private Boolean enable = true;

        /** 监听端口 */
        private Integer port = 1883;
    }

    /**
     * websocket 配置
     */
    @Data
    public static class WebSocket {

        /** 开关 */
        private Boolean enable = false;

        /** 监听端口 */
        private Integer port = 8083;

        /** uri */
        private String path = "/mqtt";
    }

    /**
     * topic 过滤器，客户端订阅时，支持按前缀过滤
     */
    @Data
    public static class FilterTopic {

        /** 开启前缀匹配开关 */
        private Boolean enablePrefix = false;

        /** topic 前缀 */
        private Set<String> topicPrefix = new HashSet<>();

        /** 开启后缀匹配开关 */
        private Boolean enableSuffix = false;

        /** topic 后缀 */
        private Set<String> topicSuffix = new HashSet<>();

        /** 主题安全订阅开关，默认关 */
        private Boolean enableTopicSubPubSecure = false;
    }

    /**
     * 系统配置
     */
    @Data
    public static class SysConfig {

        /** mqttx 接收 PUB_ACK 超时时间, 默认 10 秒 */
        private Integer pubAckTimeout = 10;

        /** 批量保存消息线程大小 */
        private Integer batchSaveMsgPoolSize = 4;

        /** 批量保存消息队列大小, 建议 500 <= 配置值 <= 2000 */
        private Integer batchSaveMsgQueueSize = 1100;

        /** 批量保存消息集合大小, 建议比队列至少小 100 */
        private Integer batchSaveMsgArraySize = 1000;

        /** 批量保存消息计数器, 建议 500 <= 配置值 < 1500 */
        private Integer batchSaveMsgCounter = 1000;

        /** 批量补发离线消息数量, 默认: 500条 */
        private Integer batchSendMsgSize = 500;

        /** 单节点TCP最大连接数, 默认不开启该限制条件 */
        private Integer maxConnectSize = -1;

        /** 开启读取 PROXY IP:PORT 地址 */
        private Boolean enableProxyAddr = false;

        /** 是否开启业务日志打印 */
        private Boolean enableLog = false;

        /** 是否开启payload日志打印 */
        private Boolean enablePayloadLog = false;

        /** 优雅启动时间, 默认30秒 */
        private Integer gracefulStartTime = 30;

        /** MQTT payload 数据日志打印类型, 支持两种类型: text / hex */
        private String payloadLogType = "text";
    }

    /**
     * 共享 topic 配置
     *
     * 共享 topic 支持, 实现参考 MQTT v5, 默认关。目前仅支持根据发送端 clientId 进行 hash 后的共享策略，
     * 实现见 {@link PublishHandler} <code>chooseClient(List,String)</code> 方法.
     */
    @Data
    public static class ShareTopic {

        /** 开关 */
        private Boolean enable = true;

        /**
         * 共享订阅消息分发策略, 默认轮询
         * <ul>
         *     <li>{@link ShareStrategyEnum#random} 随机</li>
         *     <li>{@link ShareStrategyEnum#hash}  哈希</li>
         *     <li>{@link ShareStrategyEnum#round} 轮询</li>
         * </ul>
         * @see ShareStrategyEnum
         */
        private ShareStrategyEnum shareSubStrategy = ShareStrategyEnum.round;
    }

    /**
     * 系统管理 topic
     *
     * <ol>
     *     <li>当应用重启时，会丢失订阅信息，如有需要则应该重新发起系统管理主题的订阅</li>
     *     <li>当 {@link FilterTopic#enableTopicSubPubSecure} 开启时，系统管理主题也会被保护</li>
     * </ol>
     * topic 写死在 {@link TopicUtils}
     */
    @Data
    public static class SysTopic {

        /** 开关 */
        private Boolean enable = false;

        /** 定时发送时间，默认一分钟 */
        private Duration interval = Duration.ofMinutes(1);
    }

    /**
     * 消息桥接配置
     *
     */
    @Data
    public static class MessageBridge {

        /** 桥接业务消息开关 */
        private Boolean enableBiz = false;

        /** 需要桥接消息的MQTT主题正则 */
        private String mqttBizTopicPattern;

        /** 需要桥接业务消息到Kafka的主题 */
        private String kafkaBizBridgeTopic = "mqttx-bridge-biz";

        /** 桥接系统消息开关 */
        private Boolean enableSys = false;

        /** 需要桥接系统消息到Kafka的主题 */
        private String kafkaSysBridgeTopic = "mqttx-bridge-sys";
    }

    /**
     * 主题限流配置
     */
    @Data
    public static class RateLimiter {

        /** 开关 */
        private Boolean enable = false;

        /** 限流主题配置 */
        private Set<TopicRateLimit> topicRateLimits;
    }

    @Data
    public static class Auth {

        /** 开关 */
        private Boolean enable = false;

        /** 管理员账号名配置 **/
        private List<String> adminUser;

        /** 管理员客户端ID前缀 */
        private List<String> adminClientIdPrefix = new ArrayList<>();

        /** 鉴权地址信息(key表示客户端ID前缀、value表示对应的鉴权地址) */
        private Map<String, String> endpointMap = new HashMap<>();

        /** 类似 readTimeout, 见 {@link java.net.http.HttpRequest#timeout()} */
        private Duration timeout = Duration.ofSeconds(5);

        /** 客户端ID黑名单 */
        private Set<String> clientBlackList = new HashSet<>();
    }
}
