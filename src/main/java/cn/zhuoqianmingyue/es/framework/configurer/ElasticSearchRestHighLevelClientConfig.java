package cn.zhuoqianmingyue.es.framework.configurer;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

/**
 * ElasticSearch 配置类
 *
 * @author lijk
 * @date 2021/07/07
 **/
@Configuration
public class ElasticSearchRestHighLevelClientConfig {

    @Value("${spring.elasticsearch.nodes:127.0.0.1:9200}")
    private String nodes;

    /**
     * 高版本客户端
     * @return
     */
    @Bean
    public RestHighLevelClient restHighLevelClient() {
        HttpHost[] httpHostArray = nodesToArray(nodes);
        // 创建RestHighLevelClient客户端
        return new RestHighLevelClient(RestClient.builder(httpHostArray));
    }
    /**
     * 低级的客户端
     * @return
     */
    @Bean
    public RestClient restClient() {
        HttpHost[] httpHostArray = nodesToArray(nodes);
        return RestClient.builder(httpHostArray).build();
    }

    /**
     * 节点字符串转换为节点HttpHost 数组
     * @param nodes 节点字符串
     * @return HttpHost[]
     */
    private HttpHost[] nodesToArray(String nodes) {
        Assert.hasText(nodes, "nodes is empty");
        String[] split = nodes.split(",");

        HttpHost[] httpHostArray = new HttpHost[split.length];
        for (int i = 0; i < split.length; i++) {
            String node = split[i];
            Assert.hasText(node, "node is empty");
            httpHostArray[i] = new HttpHost(node.split(":")[0], Integer.parseInt(node.split(":")[1]), "http");
        }
        return httpHostArray;
    }
}
