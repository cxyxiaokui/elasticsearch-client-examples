package cn.zhuoqianmingyue.es.framework.configurer;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 创建 JestClient 工厂类
 *
 * @author lijk
 * @date 2021/07/08
 **/
public class ElasticSearchJestClientFactory {

    /**
     * 获取不需要认证的 JestClient
     * @param nodes ES 节点地址
     * @return JestHttpClient
     */
    public static JestHttpClient getClient(String nodes) {
        List<String> hostList = nodesToList(nodes);

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(
                hostList).multiThreaded(true).maxTotalConnection(300).defaultMaxTotalConnectionPerRoute(100).maxConnectionIdleTime(60, TimeUnit.SECONDS).build());
        return (JestHttpClient) factory.getObject();
    }

    /**
     * 获取需要认证的 JestClient
     * @param nodes ES 节点地址
     * @param userName 用户名称
     * @param password 密码
     * @return JestHttpClient
     */
    public static JestHttpClient getUserClient(String nodes, String userName, String password) {
        List<String> hostList = nodesToList(nodes);

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(
                hostList).defaultCredentials(userName, password).multiThreaded(true).build());

        return (JestHttpClient) factory.getObject();
    }

    /**
     * ES 节点地址字符串转集合
     *
     * @param nodes ES 节点地址
     * @return List<String>
     */
    private static List<String> nodesToList(String nodes) {
        Assert.hasText(nodes, "nodes is empty");
        String[] nodeArray = nodes.split(",");
        for (String node : nodeArray) {
            Assert.hasText(nodes, "node is empty");
        }
        return Arrays.asList(nodeArray);
    }
}
