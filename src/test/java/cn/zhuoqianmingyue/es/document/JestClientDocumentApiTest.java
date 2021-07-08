package cn.zhuoqianmingyue.es.document;

import cn.hutool.core.map.MapUtil;
import cn.zhuoqianmingyue.es.Application;
import cn.zhuoqianmingyue.es.index.RestHighLevelClientIndexApiTest;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
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
 * JestClient Api 单元测试类
 *
 * @author lijk
 * @date 2021/07/08
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class JestClientDocumentApiTest {

    private static final Logger log = LoggerFactory.getLogger(RestHighLevelClientIndexApiTest.class);
    @Autowired
    private JestClient jestClient;


    /**
     * 添加文档测试
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
     *
     * @throws IOException
     */
    @Test
    public void addTest() throws IOException {
        Map<Object, Object> build = MapUtil.builder()
                .put("name", "Java 中级课程")
                .put("price", "29")
                .put("studymodel", "offline")
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "Java 中级开发").build();
        Index index = new Index.Builder(build).index("zhuoqianmingyue_test").type("_doc").id("1").build();
        JestResult result = jestClient.execute(index);
        Assert.assertTrue(result.isSucceeded());
    }

    /**
     * 修改文档测试
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
     *
     * @throws IOException
     */
    @Test
    public void updateTest() throws IOException {
        Map<Object, Object> build = MapUtil.builder()
                .put("name", "Java 中级课程")
                .put("price", "29")
                .put("studymodel", "offline")
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "Java 中级开发").build();
        Index index = new Index.Builder(build).index("zhuoqianmingyue_test").type("_doc").id("1").build();
        JestResult result = jestClient.execute(index);
        Assert.assertTrue(result.isSucceeded());
    }

    /**
     * 修改文档测试
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
     *
     * @throws IOException
     */
    @Test
    public void deleteTest() throws IOException {
        Delete delete = new Delete.Builder("1").index("zhuoqianmingyue_test").type("_doc").build();
        JestResult result = jestClient.execute(delete);
        Assert.assertTrue(result.isSucceeded());
    }

    /**
     * 通过Id 进行查询
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html
     *
     * @throws IOException
     */
    @Test
    public void getById() throws IOException {
        Get get = new Get.Builder("zhuoqianmingyue_test", "1").type("_doc").build();
        JestResult result = jestClient.execute(get);
        Assert.assertTrue(result.isSucceeded());
    }
}
