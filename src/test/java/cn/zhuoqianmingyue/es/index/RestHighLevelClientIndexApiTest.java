package cn.zhuoqianmingyue.es.index;

import cn.zhuoqianmingyue.es.Application;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Map;

/**
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.x/java-rest-high.html
 * 索引相关操作API 单元测试类
 * @author lijk
 * @date 2021/07/07
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class RestHighLevelClientIndexApiTest {

    private static final Logger log = LoggerFactory.getLogger(RestHighLevelClientIndexApiTest.class);
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 查询索引信息 Test
     * 文档 https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-index.html
     * GET /索引名称
     * GET 127.0.0.1:9200/索引名称 (POSTMAN )
     *
     */
    @Test
    public void queryIndexTest() throws IOException {
        GetIndexRequest request = new GetIndexRequest("kibana_sample_data_ecommerce");
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(request, RequestOptions.DEFAULT);

        String[] indices = getIndexResponse.getIndices();
        log.info("索引名称 {}", JSON.toJSONString(indices));
        Map<String, MappingMetaData> mappings = getIndexResponse.getMappings();
        log.info("索引 Mapping 信息 {}", JSON.toJSONString(mappings));
    }

    /**
     * 添加索引库
     * 文档 https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
     *
      PUT /索引名称
      {
        "settings": {
          "number_of_shards": 1
        },
        "mappings": {
          "properties": {
            "field1": { "type": "text" }
          }
        }
      }
     */
    @Test
    public void addIndexTest() throws IOException {
        //添加索引库
        CreateIndexRequest request = new CreateIndexRequest("zhuoqianmingyue_test");
        //设置分片 和副本
        request.settings(Settings.builder().put("number_of_shards", "1").put("number_of_replicas", "0"));
        //设置Mapping
        // 创建映射
        request.mapping( "{\n" +
                "  \"properties\": {\n" +
                "    \"description\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\",\n" +
                "      \"search_analyzer\": \"ik_smart\"\n" +
                "    },\n" +
                "    \"name\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\",\n" +
                "      \"search_analyzer\": \"ik_smart\"\n" +
                "    },\n" +
                "    \"pic\": {\n" +
                "      \"type\": \"text\",\n" +
                "      \"index\": false\n" +
                "    },\n" +
                "    \"price\": {\n" +
                "      \"type\": \"double\"\n" +
                "    },\n" +
                "    \"studymodel\": {\n" +
                "      \"type\": \"keyword\"\n" +
                "    },\n" +
                "    \"teachers\" : {\n" +
                "      \"type\" : \"text\",\n" +
                "      \"fields\" : {\n" +
                "        \"keyword\" : {\n" +
                "          \"type\" : \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"create_date\": {\n" +
                "      \"type\": \"date\",\n" +
                "      \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||yyyy-MM-dd'T'HH:mm:ss+0800\"\n" +
                "    }\n" +
                "  }\n" +
                "}", XContentType.JSON);

        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = createIndexResponse.isAcknowledged();
        Assert.assertTrue(acknowledged);
    }

    /**
     * 删除索引库
     * 文档 https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html
     *
     * DELETE /索引名称
     */
    @Test
    public void deleteIndexTest() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("zhuoqianmingyue_test");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        boolean acknowledged = delete.isAcknowledged();
        Assert.assertTrue(acknowledged);
    }


}
