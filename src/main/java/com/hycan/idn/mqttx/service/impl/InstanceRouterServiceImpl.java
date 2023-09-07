package com.hycan.idn.mqttx.service.impl;

import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.NacosServiceManager;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.exception.AuthenticationException;
import com.hycan.idn.mqttx.pojo.AdminClient;
import com.hycan.idn.mqttx.service.IInstanceRouterService;
import com.hycan.idn.mqttx.utils.ExceptionUtil;
import io.netty.channel.ChannelOutboundInvoker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author shichongying
 */
@Slf4j
@Service
public class InstanceRouterServiceImpl implements IInstanceRouterService {

    private static final int REBALANCED_SIZE = 2;

    private final ReactiveStringRedisTemplate stringRedisTemplate;
    private final NacosServiceManager nacosServiceManager;
    private final NacosDiscoveryProperties nacosDiscoveryProperties;

    private final String adminConnHashPrefix, currentInstanceId;

    private final List<String> adminClientIdPrefix;

    public InstanceRouterServiceImpl(ReactiveStringRedisTemplate stringRedisTemplate,
                                     NacosServiceManager nacosServiceManager,
                                     NacosDiscoveryProperties nacosDiscoveryProperties,
                                     MqttxConfig config) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.nacosServiceManager = nacosServiceManager;
        this.nacosDiscoveryProperties = nacosDiscoveryProperties;

        this.currentInstanceId = config.getInstanceId();

        MqttxConfig.Redis redis = config.getRedis();
        this.adminConnHashPrefix = redis.getAdminConnHashPrefix();
        this.adminClientIdPrefix = config.getAuth().getAdminClientIdPrefix();
        if (CollectionUtils.isEmpty(adminClientIdPrefix)) {
            throw new IllegalArgumentException(format("mqttx.auth.adminClientIdPrefix [%s] 配置为空", adminClientIdPrefix));
        }
    }

    /**
     * 自动均衡管理员客户端定时器
     */
    @Scheduled(initialDelay = 30, fixedDelay = 60, timeUnit = TimeUnit.SECONDS)
    private void autoRebalancedAdminClientConnect() {
        if (getActiveInstances().size() < REBALANCED_SIZE) {
            return;
        }

        stringRedisTemplate.opsForHash().entries(adminConnHashPrefix)
                .map(entry -> AdminClient.of(entry.getKey().toString(), entry.getValue().toString()))
                .collectList()
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(adminClients -> {
                    for (String prefix : adminClientIdPrefix) {
                        Map<String, List<String>> adminClientMap = convertAdminClientMapByPrefix(adminClients, prefix);
                        if (CollectionUtils.isEmpty(adminClientMap)) {
                            continue;
                        }

                        List<String> currentConnectClientList = adminClientMap.get(currentInstanceId);
                        if (CollectionUtils.isEmpty(currentConnectClientList)) {
                            continue;
                        }

                        int minConnectClientSize = 0;
                        Optional<Map.Entry<String, List<String>>> minEntryOptional = adminClientMap.entrySet().stream()
                                .filter(entry -> !Objects.equals(entry.getKey(), currentInstanceId))
                                .min(Comparator.comparingInt(entry -> entry.getValue().size()));
                        if (minEntryOptional.isPresent()) {
                            minConnectClientSize = minEntryOptional.get().getValue().size();
                        }

                        if ((currentConnectClientList.size() - minConnectClientSize) < REBALANCED_SIZE) {
                            continue;
                        }

                        for (int index = 0; index < (currentConnectClientList.size() - minConnectClientSize - 1); index++) {
                            String clientId = currentConnectClientList.get(index);
                            if (!ConnectHandler.CLIENT_MAP.containsKey(clientId)) {
                                continue;
                            }

                            stringRedisTemplate.opsForHash().remove(adminConnHashPrefix, clientId)
                                    .doOnSuccess(unused -> Optional.ofNullable(clientId)
                                            .map(ConnectHandler.CLIENT_MAP::get)
                                            .map(BrokerHandler.CHANNELS::find)
                                            .map(ChannelOutboundInvoker::close))
                                    .subscribe();
                            log.info("强制断开: 管理员客户端ID=[{}]需要重平衡到其他MQTTX节点!", clientId);
                        }
                    }
                }).subscribe();
    }

    /**
     * admin客户端连接路由策略，不满足时抛异常拒绝连接
     *
     * @param clientId 当前发起连接的admin客户端ID
     * @return true/false
     */
    @Override
    public Mono<Boolean> adminClientConnectRouter(String clientId) {
        if (getActiveInstances().size() < REBALANCED_SIZE) {
            return Mono.just(Boolean.TRUE);
        }

        return stringRedisTemplate.opsForHash().entries(adminConnHashPrefix)
                .map(entry -> AdminClient.of(entry.getKey().toString(), entry.getValue().toString()))
                .collectList()
                .publishOn(Schedulers.boundedElastic())
                .map(adminClients -> {
                    String clientIdPrefix = getAdminClientIdPrefix(clientId);
                    Map<String, List<String>> adminClientMap = convertAdminClientMapByPrefix(adminClients, clientIdPrefix);
                    int currentAdminClientSize = ConnectHandler.CLIENT_MAP.entrySet().stream()
                            .filter(entry -> entry.getKey().startsWith(clientIdPrefix))
                            .toList().size();
                    return isAdminClientConnectAccess(adminClientMap, currentAdminClientSize);
                });
    }

    /**
     * 清理admin客户端连接信息
     * @param clientId 当前发起断开连接的admin客户端ID
     * @return VOID
     */
    @Override
    public Mono<Void> clearAdminClientConnect(String clientId) {
        Optional<String> clientIdPrefixOptional = adminClientIdPrefix.stream().filter(clientId::startsWith).findFirst();
        if (clientIdPrefixOptional.isPresent()) {
            return stringRedisTemplate.opsForHash().remove(adminConnHashPrefix, clientId).then();
        }
        return Mono.empty();
    }

    /**
     * 获取当前发起连接的admin客户端ID前缀
     *
     * @param clientId 当前发起连接的admin客户端ID
     * @return admin客户端ID前缀
     */
    private String getAdminClientIdPrefix(String clientId) {
        Optional<String> clientIdPrefixOptional = adminClientIdPrefix.stream().filter(clientId::startsWith).findFirst();
        if (clientIdPrefixOptional.isPresent()) {
            return clientIdPrefixOptional.get();
        }
        throw new AuthenticationException(format("鉴权失败: 不支持的客户端ID前缀, 客户端ID=[%s]", clientId));
    }

    /**
     * 将redis中获取到的admin客户端列表，按当前连接的客户端前缀筛选，并按照hostName和clientIds转换成map结构
     *
     * @param adminClients   redis中获取到的admin客户端列表
     * @param clientIdPrefix 客户端ID前缀
     * @return 按hostName和clientIds转换后的map结构
     */
    private Map<String, List<String>> convertAdminClientMapByPrefix(List<AdminClient> adminClients, String clientIdPrefix) {
        Map<String, List<String>> adminClientMap = new HashMap<>();
        for (var adminClient : adminClients) {
            String clientId = adminClient.getClientId();
            String instanceId = adminClient.getInstanceId();
            // 从redis中获取到的admin客户端ID中，筛选出与当前客户端ID前缀匹配的列表
            if (!clientId.startsWith(clientIdPrefix)) {
                continue;
            }

            // 将未与任何节点建立连接的admin客户端ID从redis删除
            if (!ConnectHandler.ALL_CLIENT_MAP.containsKey(clientId)) {
                stringRedisTemplate.opsForHash().remove(adminConnHashPrefix, clientId).subscribe();
                continue;
            }

            if (adminClientMap.containsKey(instanceId)) {
                adminClientMap.get(instanceId).add(clientId);
            } else {
                List<String> clientIds = new ArrayList<>();
                clientIds.add(clientId);
                adminClientMap.put(instanceId, clientIds);
            }
        }

        return adminClientMap;
    }

    /**
     * 校验是否允许与当前节点建立连接
     *
     * @param adminClientMap         按hostName和clientIds转换后的map结构
     * @param currentAdminClientSize 与当前客户端ID前缀匹配，且与当前节点建立连接的数量
     * @return true/false
     */
    private boolean isAdminClientConnectAccess(Map<String, List<String>> adminClientMap, int currentAdminClientSize) {
        // 当前节点没有匹配前缀的admin客户端连接时，允许连接
        if (!adminClientMap.containsKey(currentInstanceId)) {
            return true;
        }

        List<String> instanceIds = getActiveInstances().stream().map(Instance::getInstanceId).toList();
        if (instanceIds.stream().anyMatch(instanceId -> !adminClientMap.containsKey(instanceId))) {
            return false;
        }

        // 当前节点有匹配前缀的admin客户端连接，但是其他节点存在连接数 > 当前节点连接数，允许连接
        if (adminClientMap.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(currentInstanceId))
                .map(Map.Entry::getValue)
                .anyMatch(list -> list.size() > currentAdminClientSize)) {
            return true;
        }

        // 当前节点有匹配前缀的admin客户端连接，其他每个节点的连接数 = 当前节点连接数，允许连接
        return adminClientMap.values().stream()
                .filter(strings -> strings.size() == currentAdminClientSize)
                .anyMatch(list -> true);
    }

    /**
     * 从Nacos中获取活跃的mqttx节点数量
     * @return
     */
    private List<Instance> getActiveInstances() {
        try {
            NamingService namingService = nacosServiceManager.getNamingService(nacosDiscoveryProperties.getNacosProperties());
            List<Instance> instanceList = namingService.getAllInstances(
                    nacosDiscoveryProperties.getService(), nacosDiscoveryProperties.getGroup(),
                    Collections.singletonList(nacosDiscoveryProperties.getClusterName()));
            if (CollectionUtils.isEmpty(instanceList)) {
                log.warn("获取MQTTX实例数: Nacos地址=[{}], 命名空间ID=[{}], 分组名称=[{}], 服务名=[{}], 实例数=[{}]",
                        nacosDiscoveryProperties.getNacosProperties().getProperty("serverAddr"),
                        nacosDiscoveryProperties.getNacosProperties().getProperty("namespace"),
                        nacosDiscoveryProperties.getNacosProperties().getProperty("group"),
                        nacosDiscoveryProperties.getService(), instanceList.size());
            }
            return instanceList;
        } catch (NacosException ne) {
            log.error("获取MQTTX实例数量异常, 异常详情=[{}]", ExceptionUtil.getBriefStackTrace(ne));
            return new ArrayList<>();
        }
    }
}
