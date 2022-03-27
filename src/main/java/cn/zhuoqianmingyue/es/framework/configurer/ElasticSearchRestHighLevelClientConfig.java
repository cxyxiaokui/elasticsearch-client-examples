package cn.zhuoqianmingyue.es.framework.configurer;

import cn.hutool.core.util.StrUtil;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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
     * 设置 ElasticSearch 带密码配置
     * @return
     */
//    @Bean
//    public RestHighLevelClient highLevelClient() {
//
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        //测试环境es无需配置用户名密码  线上环境阿里es需配置用户名密码
//        String userName = "";
//        String password = "";
//        if(StrUtil.isNotEmpty(userName) && StrUtil.isNotEmpty(password)){
//            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
//        }
//
//        RestClientBuilder builder = RestClient.builder(new HttpHost("", 2323))
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                });
//
//        return new RestHighLevelClient(builder);
//
//    }

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
