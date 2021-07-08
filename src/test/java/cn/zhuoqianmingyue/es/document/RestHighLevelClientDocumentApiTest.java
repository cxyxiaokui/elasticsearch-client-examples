package cn.zhuoqianmingyue.es.document;

import cn.zhuoqianmingyue.es.Application;
import cn.zhuoqianmingyue.es.index.RestHighLevelClientIndexApiTest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

/**
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.x/java-rest-high.html
 * 文档相关操作API 单元测试类
 *
 * @author lijk
 * @date 2021/07/07
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class RestHighLevelClientDocumentApiTest {

    private static final Logger log = LoggerFactory.getLogger(RestHighLevelClientIndexApiTest.class);
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 添加文档测试
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
     *
     * @throws IOException
     */
    @Test
    public void addTest() throws IOException {

        IndexRequest request = new IndexRequest("zhuoqianmingyue_test");
        request.id("1");
        String jsonString = "{" +
                "\"description\":\"Bootstrap是由Twitter推出的一个前台页面开发框架，是一个非常流行的开发框架，此框架集成了多种页面效果。此开发框架包含了大量的CSS、JS程序代码，可以帮助开发者（尤其是不擅长页面开发的程序人员）轻松的实现一个不受浏览器限制的精美界面效果\"," +
                "\"name\":\"Bootstrap开发\"," +
                "\"pic\":\"group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg\"," +
                "\"price\":\"38.6\"," +
                "\"studymodel\":\"201002\"," +
                "\"create_date\":\"2012-09-01 00:00:00\"" +
                "}";

        request.source(jsonString, XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        RestStatus status = index.status();
        Assert.assertTrue(RestStatus.CREATED.equals(status) || RestStatus.OK.equals(status));
    }

    /**
     * 修改文档测试
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
     *
     * @throws IOException
     */
    @Test
    public void updateTest() throws IOException {

        String jsonString = "{" +
                "\"description\":\"Bootstrap是由Twitter推出的一个前台页面开发框架，是一个非常流行的开发框架，此框架集成了多种页面效果。此开发框架包含了大量的CSS、JS程序代码，可以帮助开发者（尤其是不擅长页面开发的程序人员）轻松的实现一个不受浏览器限制的精美界面效果\"," +
                "\"name\":\"Bootstrap开发\"," +
                "\"pic\":\"group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg\"," +
                "\"price\":\"39.6\"," +
                "\"create_date\":\"2012-09-01 00:00:00\"" +
                "}";

        UpdateRequest updateRequest = new UpdateRequest("zhuoqianmingyue_test", "1");
        updateRequest.doc(jsonString, XContentType.JSON);

        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        RestStatus status = update.status();
        Assert.assertTrue(RestStatus.OK.equals(status));
    }

    /**
     * 修改文档测试
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
     *
     * @throws IOException
     */
    @Test
    public void deleteTest() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("zhuoqianmingyue_test", "1");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        RestStatus status = delete.status();
        Assert.assertTrue(RestStatus.OK.equals(status));
    }

    /**
     * 通过Id 进行查询
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html
     *
     * @throws IOException
     */
    @Test
    public void getById() throws IOException {
        GetRequest getRequest = new GetRequest("zhuoqianmingyue_test", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        String sourceAsString = getResponse.getSourceAsString();
        log.info("sourceAsString: {}", sourceAsString);
    }
}
