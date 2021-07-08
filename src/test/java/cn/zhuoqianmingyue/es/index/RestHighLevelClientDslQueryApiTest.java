package cn.zhuoqianmingyue.es.index;

import cn.zhuoqianmingyue.es.Application;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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
 *
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
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "create_date"}, new String[]{});
        int page = 2; // 页码
        int size = 2; // 每页显示的条数
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
}
