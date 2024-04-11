package com.hycan.idn.mqttx.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hycan.idn.mqttx.broker.BrokerHandler;
import com.hycan.idn.mqttx.broker.handler.ConnectHandler;
import com.hycan.idn.mqttx.config.MqttxConfig;
import com.hycan.idn.mqttx.controller.pojo.MqttClientInfoRspVO;
import com.hycan.idn.mqttx.pojo.ClientSub;
import com.hycan.idn.mqttx.service.ISubscriptionService;
import com.hycan.idn.mqttx.utils.ExceptionUtil;
import com.hycan.idn.mqttx.utils.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.Size;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.hycan.idn.mqttx.graceful.InstanceChangeListener.ACTIVE_INSTANCE_SET;

/**
 * 内部接口，获取集群中的缓存数据
 *
 * @author Shadow
 * @datetime 2023-11-02 19:41
 */
@Slf4j
@Validated
@RestController
@RequestMapping("/api/v1/inner")
public class InnerController {

    private static final String INTERNAL_URI_SUB = "/api/v1/inner/clients?is_cluster_req=true";
    private static final String INTERNAL_URI_CLIENT = "/api/v1/inner/clients?is_cluster_req=true";
    // 第一个%s为ip  第二个%s为port
    private static final String ENDPOINT = "http://%s:%s";

    private final HttpClient httpClient;

    /**
     * 订阅服务
     */
    private final ISubscriptionService subscriptionService;

    private final String currentInstanceId;

    public InnerController(MqttxConfig mqttxConfig,
                           HttpClient httpClient,
                           ISubscriptionService subscriptionService) {
        this.httpClient = httpClient;
        this.subscriptionService = subscriptionService;

        this.currentInstanceId = mqttxConfig.getInstanceId();
    }

    /**
     * 获取集群内部各实例订阅信息
     *
     * @param isClusterReq 是否为集群实例发起的请求，默认：false
     * @param topics       topic列表
     * @return 各实例订阅信息
     */
    @PostMapping("/subs")
    public ResponseEntity<Map<String, Map<String, List<ClientSub>>>> subscription(
            @RequestParam(value = "is_cluster_req", defaultValue = "false") boolean isClusterReq,
            @RequestBody @Valid @Size(min = 1, max = 100) List<String> topics) {
        Map<String, Map<String, List<ClientSub>>> subMap = new HashMap<>(ACTIVE_INSTANCE_SET.size());
        Map<String, List<ClientSub>> voMap = new HashMap<>(topics.size());
        for (String topic : topics) {
            List<ClientSub> clientSubs = subscriptionService.searchSubscribeClientList(topic).collectList().block();
            voMap.put(topic, clientSubs);
        }
        subMap.put(currentInstanceId, voMap);

        sendInternalRequest(isClusterReq, INTERNAL_URI_SUB, topics, subMap);

        return ResponseEntity.ok(subMap);
    }

    /**
     * 获取集群内部各实例客户端信息
     *
     * @param isClusterReq 是否为集群实例发起的请求，默认：false
     * @param clientIds    客户端ID列表
     * @return 各实例客户端信息
     */
    @PostMapping("/clients")
    public ResponseEntity<Map<String, Map<String, MqttClientInfoRspVO>>> client(
            @RequestParam(value = "is_cluster_req", defaultValue = "false") boolean isClusterReq,
            @RequestBody @Valid @Size(min = 1, max = 10) List<String> clientIds) {
        Map<String, Map<String, MqttClientInfoRspVO>> clientMap = new HashMap<>(ACTIVE_INSTANCE_SET.size());
        Map<String, MqttClientInfoRspVO> voMap = new HashMap<>(clientIds.size());
        for (String clientId : clientIds) {
            MqttClientInfoRspVO vo = new MqttClientInfoRspVO();
            vo.setIsSync(ConnectHandler.ALL_CLIENT_MAP.containsKey(clientId));
            vo.setChannelId(Optional.ofNullable(ConnectHandler.CLIENT_MAP.get(clientId))
                    .map(BrokerHandler.CHANNELS::find)
                    .map(channel -> channel.id().asShortText()).orElse(null));
            vo.setClientSubs(subscriptionService.getAllClientSubs().stream()
                    .filter(clientSub -> clientId.equals(clientSub.getClientId()))
                    .toList());
            voMap.put(clientId, vo);
        }
        clientMap.put(currentInstanceId, voMap);
        sendInternalRequest(isClusterReq, INTERNAL_URI_CLIENT, clientIds, clientMap);
        return ResponseEntity.ok(clientMap);
    }

    /**
     * 向集群中的实例发送请求，获取各实例的数据
     *
     * @param isClusterReq 是否为集群实例发起的请求
     * @param uri          请求URI
     * @param params       请求Body参数
     * @param result       组装完成的响应数据
     * @param <T>          入参对象泛型
     */
    private <T> void sendInternalRequest(boolean isClusterReq, String uri, List<String> params, Map<String, Map<String, T>> result) {
        if (isClusterReq) {
            return;
        }

        for (String instanceId : ACTIVE_INSTANCE_SET) {
            String response = sendRequest(instanceId, uri, params);
            if (!StringUtils.hasText(response)) {
                continue;
            }

            result.putAll(JSON.readValue(response, new TypeReference<>() {
            }));
        }
    }

    /**
     * 发送HTTP请求（这段代码有一点定制化）
     *
     * @param instanceId 实例ID
     * @param uri        请求URI
     * @param params     请求Body参数
     * @return 响应JSON数据
     */
    private String sendRequest(String instanceId, String uri, List<String> params) {
        if (instanceId.equals(currentInstanceId)) {
            return null;
        }

        String endpoint = getEndpoint(instanceId);
        if (!StringUtils.hasText(endpoint)) {
            return null;
        }

        try {
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint + uri))
                    .timeout(Duration.ofSeconds(5))
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(params))).build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (HttpStatus.OK.value() == response.statusCode()) {
                return response.body();
            }
        } catch (Exception e) {
            log.error("集群内部调用接口异常=[{}]", ExceptionUtil.getBriefStackTrace(e));
        }
        return null;
    }

    /**
     * 获取实例的连接地址
     *
     * @param instanceId 实例ID，格式：10.10.40.63#8884#DEFAULT#DEFAULT_GROUP@@mqttx
     * @return 连接地址，格式：http://10.10.40.63:8884
     */
    private String getEndpoint(String instanceId) {
        String[] parts = instanceId.split("#");
        if (parts.length >= 2) {
            String ip = parts[0];
            String port = parts[1];
            return String.format(ENDPOINT, ip, port);
        }
        return null;
    }
}
