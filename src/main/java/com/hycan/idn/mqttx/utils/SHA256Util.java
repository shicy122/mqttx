package com.hycan.idn.mqttx.utils;

import com.hycan.idn.mqttx.exception.EncryptException;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * SHA256加密工具类
 *
 * @author shadow
 * @date 2023年01月14日
 */
@Slf4j
public class SHA256Util {
    public static final String ALGORITHM = "HmacSHA256";

    /**
     * SHA256加密
     *
     * @param sk              日期(yyyyMMdd)
     * @param canonicalString 待加密字符串(vin_sn_date)
     */
    public static String signWithHmacSha256(String sk, String canonicalString) {
        try {
            SecretKeySpec signingKey = new SecretKeySpec(sk.getBytes(StandardCharsets.UTF_8), ALGORITHM);
            Mac mac = Mac.getInstance(ALGORITHM);

            mac.init(signingKey);
            return Base64.getEncoder()
                    .encodeToString(mac.doFinal(canonicalString.getBytes(StandardCharsets.UTF_8)))
                    .replaceAll("\n","");
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            log.error("SHA256加密异常，异常信息={}", e.getMessage());
            throw new EncryptException("encrypt error!");
        }
    }
}
