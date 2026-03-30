
package com.ityj;

import com.ityj.feishu.bitable.config.FeishuBitableProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@EnableConfigurationProperties(FeishuBitableProperties.class)
public class MainStarter {
    public static void main(String[] args) {
        SpringApplication.run(MainStarter.class, args);
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}