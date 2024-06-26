package com.hycan.idn.mqttx.config;

import com.hycan.idn.mqttx.constants.SerializeStrategy;
import com.hycan.idn.mqttx.utils.JsonSerializer;
import com.hycan.idn.mqttx.utils.KryoSerializer;
import com.hycan.idn.mqttx.utils.Serializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SerializeConfig {

    @Bean
    @ConditionalOnProperty(prefix = "mqttx", name = "serialize-strategy", havingValue = SerializeStrategy.JSON)
    public Serializer jsonSerializer() {
        return new JsonSerializer();
    }

    @Bean
    @ConditionalOnProperty(prefix = "mqttx", name = "serialize-strategy", havingValue = SerializeStrategy.KRYO)
    public Serializer kryoSerializer() {
        return new KryoSerializer();
    }
}
