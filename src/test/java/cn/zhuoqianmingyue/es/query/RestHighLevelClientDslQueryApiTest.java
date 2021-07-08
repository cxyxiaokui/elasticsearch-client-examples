package cn.zhuoqianmingyue.es.query;

import cn.hutool.core.map.MapUtil;
import cn.zhuoqianmingyue.es.Application;
import cn.zhuoqianmingyue.es.index.RestHighLevelClientIndexApiTest;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
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
 * RestHightLevelClientQueryApiTest
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.x/java-rest-high.html
 * @author lijk
 * @date 2021/07/07
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class RestHighLevelClientDslQueryApiTest {

    private static final Logger log = LoggerFactory.getLogger(RestHighLevelClientIndexApiTest.class);
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 查询全部 matchAll
     * <p>
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html
     *
     * @throws IOException
     */
    @Test
    public void matchAllQueryTest() throws IOException {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //设置要显示的字段
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "create_date"}, new String[]{});
        searchRequest.source(searchSourceBuilder);

        //ES发起http请求
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            log.info(hit.getSourceAsString());
        }
    }

    /**
     * 分页+ 排序 查询
     * <p>
     * 分页文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
     * 排序文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/sort-search-results.html
     *
     * @throws IOException
     */
    @Test
    public void pageQueryTest() throws IOException {
        // 搜索请求对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //设置要显示的字段
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "create_date"}, new String[]{});

        //设置分页的from to
        // 页码
        int page = 2;
        // 每页显示的条数
        int size = 2;
        int index = (page - 1) * size;
        searchSourceBuilder.from(index);
        searchSourceBuilder.size(size);
        searchSourceBuilder.sort("_id", SortOrder.DESC);

        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            log.info("id:{} ,sourceAsString:{}", id, hit.getSourceAsString());
        }
    }

    /**
     * 精确查询
     * 文档 https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
     *
     * @throws IOException
     */
    @Test
    public void termQueryTest() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("studymodel", "online"));

        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            log.info("id:{} ,sourceAsString:{}", id, hit.getSourceAsString());
        }
    }

    /**
     * 单个字段全文检索查询
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
     */
    @Test
    public void matchQueryTest() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "Java"));

        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            log.info("id:{} ,sourceAsString:{}", id, hit.getSourceAsString());
        }
    }

    /**
     * 多个字段全文检索
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html
     */
    @Test
    public void multiQueryQueryTest() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("Java", "name", "description"));

        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            log.info("id:{} ,sourceAsString:{}", id, hit.getSourceAsString());
        }
    }

    /**
     * 过滤器查询
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/filter-search-results.html
     */
    @Test
    public void filterQueryTest() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("Java", "name", "description");
        searchSourceBuilder.query(multiMatchQueryBuilder);
        searchSourceBuilder.postFilter(QueryBuilders.termQuery("studymodel", "online"));


        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            log.info("id:{} ,sourceAsString:{}", id, hit.getSourceAsString());
        }
    }

    /**
     * 布尔查询 组合查询
     * must：文档必须匹配must所包括的查询条件，相当于 “AND”
     * should：文档应该匹配should所包括的查询条件其中的一个或多个，相当于 "OR"
     * must_not：文档不能匹配must_not所包括的该查询条件，相当于“NOT”
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
     */
    @Test
    public void boolQueryTest() throws IOException {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("开发", "name", "description");
        TermQueryBuilder price = QueryBuilders.termQuery("price", "29");
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(price);

        boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel", "online"));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            log.info("id:{} ,sourceAsString:{}", id, hit.getSourceAsString());
        }
    }

    /**
     * 高亮查询
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
     */
    @Test
    public void highlightQueryTest() throws IOException {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("开发", "name", "description");
        TermQueryBuilder price = QueryBuilders.termQuery("price", "29");
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(price);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);

        // 高亮查询
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<em>"); // 高亮前缀
        highlightBuilder.postTags("</em>"); // 高亮后缀
        highlightBuilder.fields().add(new HighlightBuilder.Field("name")); // 高亮字段
        searchSourceBuilder.highlighter(highlightBuilder);

        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            highlightFields.forEach((k, v) -> {
                log.info("HighlightValueKey:{} ,HighlightValue:{}", k, v);
            });
            log.info("id:{} ,sourceAsString:{}", id, hit.getSourceAsString());
        }
    }

    @Test
    public void initTestDate() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest request0 = new IndexRequest("zhuoqianmingyue_test");
        request0.id("1");
        Map<Object, Object> build0 = MapUtil.builder()
                .put("name", "Bootstrap高级开发")
                .put("price", "29")
                .put("studymodel", "online")
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "Bootstrap 前端开发框架").build();
        String jsonString = JSON.toJSONString(build0);
        request0.source(jsonString, XContentType.JSON);
        bulkRequest.add(request0);

        IndexRequest request1 = new IndexRequest("zhuoqianmingyue_test");
        request1.id("2");
        Map<Object, Object> build1 = MapUtil.builder()
                .put("name", "Java 高级开发")
                .put("price", "28")
                .put("studymodel", "offline")
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "Java 高级课程").build();
        String jsonString1 = JSON.toJSONString(build1);
        request1.source(jsonString1, XContentType.JSON);
        bulkRequest.add(request1);

        IndexRequest request2 = new IndexRequest("zhuoqianmingyue_test");
        request2.id("3");
        Map<Object, Object> build2 = MapUtil.builder()
                .put("name", "Java 中级课程")
                .put("price", "29")
                .put("studymodel", "offline")
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "Java 中级开发").build();
        String jsonString2 = JSON.toJSONString(build2);
        request2.source(jsonString2, XContentType.JSON);
        bulkRequest.add(request2);

        IndexRequest request3 = new IndexRequest("zhuoqianmingyue_test");
        request3.id("4");
        Map<Object, Object> build3 = MapUtil.builder()
                .put("name", "PHP 中级开发")
                .put("price", "27")
                .put("studymodel", "online")
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "PHP 中级开发").build();
        String jsonString3 = JSON.toJSONString(build3);
        request3.source(jsonString3, XContentType.JSON);
        bulkRequest.add(request3);

        IndexRequest request4 = new IndexRequest("zhuoqianmingyue_test");
        request4.id("5");
        Map<Object, Object> build4 = MapUtil.builder()
                .put("name", "Java 初级开发")
                .put("price", "28")
                .put("studymodel", "online")
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "Java 初级开发").build();
        String jsonString4 = JSON.toJSONString(build4);
        request4.source(jsonString4, XContentType.JSON);
        bulkRequest.add(request4);

        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }
}
