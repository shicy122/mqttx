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

package com.hycan.idn.mqttx.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.nio.charset.StandardCharsets;

/**
 * 客户端认证对象
 *
 * @author Shadow
 * @since 1.0.5
 */
@Data
public class ClientAuth {

    @JsonProperty(value="username")
    private String username;

    @JsonProperty(value="password")
    private String password;

    @JsonProperty(value="client_id")
    private String clientId;

    public static ClientAuth of(String clientId, String username, byte[] password) {
        ClientAuth authDTO = new ClientAuth();
        authDTO.setPassword(new String(password, StandardCharsets.UTF_8));
        authDTO.setUsername(username);
        authDTO.setClientId(clientId);
        return authDTO;
    }
}
