package cn.zhuoqianmingyue.es.index;

import cn.zhuoqianmingyue.es.Application;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Cat;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.settings.GetSettings;
import org.elasticsearch.common.settings.Settings;
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
 * JestClient Api 操作测试类
 *
 * @author lijk
 * @date 2021/07/08
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class JestClientIndexApiTest {

    private static final Logger log = LoggerFactory.getLogger(RestHighLevelClientIndexApiTest.class);
    @Autowired
    private JestClient jestClient;


    /**
     * 查询索引信息 Test
     * 文档 https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-index.html
     * GET /索引名称
     * GET 127.0.0.1:9200/索引名称 (POSTMAN )
     */
    @Test
    public void queryIndexTest() throws IOException {
        Cat cat = new Cat.IndicesBuilder().addIndex("zhuoqianmingyue_test").build();
        JestResult jestResult = jestClient.execute(cat);

        GetMapping getMapping = new GetMapping.Builder().addIndex("zhuoqianmingyue_test").build();
        JestResult resultMapping = jestClient.execute(getMapping);
        Assert.assertTrue(resultMapping.isSucceeded());

        JestResult jestResultSetting = jestClient.execute(new GetSettings.Builder().addIndex("zhuoqianmingyue_test").build());
        Assert.assertTrue(jestResultSetting.isSucceeded());
    }

    /**
     * 添加索引库
     * 文档 https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
     * <p>
     * PUT /索引名称
     * {
     * "settings": {
     * "number_of_shards": 1
     * },
     * "mappings": {
     * "properties": {
     * "field1": { "type": "text" }
     * }
     * }
     * }
     */
    @Test
    public void addIndexTest() throws IOException {
        String mapping = "{\n" +
                "                \"properties\": {\n" +
                "                    \"description\": {\n" +
                "                        \"type\": \"text\",\n" +
                "                        \"analyzer\": \"ik_max_word\",\n" +
                "                        \"search_analyzer\": \"ik_smart\"\n" +
                "                    },\n" +
                "                    \"name\": {\n" +
                "                        \"type\": \"text\",\n" +
                "                        \"analyzer\": \"ik_max_word\",\n" +
                "                        \"search_analyzer\": \"ik_smart\"\n" +
                "                    },\n" +
                "\"pic\":{                    \n" +
                "\"type\":\"text\",                        \n" +
                "\"index\":false                        \n" +
                "},                    \n" +
                "                    \"price\": {\n" +
                "                        \"type\": \"float\"\n" +
                "                    },\n" +
                "                    \"studymodel\": {\n" +
                "                        \"type\": \"keyword\"\n" +
                "                    },\n" +
                "                    \"create_date\": {\n" +
                "                        \"type\": \"date\",\n" +
                "                        \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||yyyy-MM-dd'T'HH:mm:ss+0800\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }";

        String settings = Settings.builder().put("number_of_shards", "1").put("number_of_replicas", "0").build().toString();

        CreateIndex createIndex = new CreateIndex.Builder("zhuoqianmingyue_test").settings(settings).mappings(mapping).build();
        JestResult jestResult = jestClient.execute(createIndex);
        Assert.assertTrue(jestResult.isSucceeded());
    }


    /**
     * 删除索引库
     * 文档 https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html
     * <p>
     * DELETE /索引名称
     */
    @Test
    public void deleteIndexTest() throws IOException {
        JestResult jestResult = jestClient.execute(new DeleteIndex.Builder("zhuoqianmingyue_test").build());
        Assert.assertTrue(jestResult.isSucceeded());
    }

    /**
     * 发送ES 原生 DSL 查询
     *
     * @return SearchResult
     * @author lijk
     * @date 2021/06/07 14:19
     **/
    public SearchResult jsonSearch(String jsonStr, String indexName, String typeName) {
        Search search = new Search.Builder(jsonStr).addIndex(indexName).addType(typeName).build();
        try {
            return jestClient.execute(search);
        } catch (Exception e) {
            log.error("jsonSearch 获取数据异常, jsonStr={} indexName={} typeName{}", jsonStr, indexName, typeName, e);
        }

        return null;
    }
}
