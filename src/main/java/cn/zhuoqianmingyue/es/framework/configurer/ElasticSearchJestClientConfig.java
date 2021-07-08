package cn.zhuoqianmingyue.es.framework.configurer;

import io.searchbox.client.JestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Jest Client 配置类
 *
 * @author lijk
 * @date 2021/07/08
 **/
@Configuration
public class ElasticSearchJestClientConfig {

    @Value("${spring.elasticsearch.nodes:127.0.0.1:9200}")
    private String nodes;

    @Bean
    public JestClient jestClient() {
        return ElasticSearchJestClientFactory.getClient(nodes);
    }
}
