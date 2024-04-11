package com.hycan.idn.mqttx.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

/**
 * TODO 用户信息
 *
 * @author Shadow
 * @datetime 2024-02-20 10:36
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserInfo {

    /**
     * MQTT用户名
     */
    @NotBlank(message = "用户名不能为空")
    @Pattern(regexp = "^[a-z0-9A-Z_-]{5,33}$", message = "支持大小写字母、数字、下划线、横线，且长度为5~33位")
    private String username;

    /**
     * mqtt鉴权密码
     */
    @NotBlank(message = "密码不能为空")
    @Pattern(regexp = "^[a-zA-Z0-9!@#$%^&*()-_+=|\\[\\]{};:'\",.<>/?]{5,50}$", message = "密码长度必须为5~50位")
    private String password;
}
