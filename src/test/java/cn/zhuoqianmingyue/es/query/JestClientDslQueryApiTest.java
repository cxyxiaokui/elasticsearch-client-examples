package cn.zhuoqianmingyue.es.query;

import cn.zhuoqianmingyue.es.Application;
import cn.zhuoqianmingyue.es.index.RestHighLevelClientIndexApiTest;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * JestClient 客户端 Dsl Query 单元测试类
 *
 * @author lijk
 * @date 2021/07/08
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class JestClientDslQueryApiTest {

    private static final Logger log = LoggerFactory.getLogger(RestHighLevelClientIndexApiTest.class);
    @Autowired
    private JestClient jestClient;


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
        String queryString = searchSourceBuilder.toString();
        SearchResult searchResult = jsonSearch(queryString, "zhuoqianmingyue_test", "_doc");
        List<SearchResult.Hit<JSONObject, Void>> hits = searchResult.getHits(JSONObject.class);
        Assert.assertNotNull(hits);
        log.info("查询完毕！");
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
        String queryString = searchSourceBuilder.toString();
        SearchResult searchResult = jsonSearch(queryString, "zhuoqianmingyue_test", "_doc");
        List<SearchResult.Hit<JSONObject, Void>> hits = searchResult.getHits(JSONObject.class);
        Assert.assertNotNull(hits);
        log.info("查询完毕！");
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
        String queryString = searchSourceBuilder.toString();
        SearchResult searchResult = jsonSearch(queryString, "zhuoqianmingyue_test", "_doc");
        List<SearchResult.Hit<JSONObject, Void>> hits = searchResult.getHits(JSONObject.class);
        Assert.assertNotNull(hits);
        log.info("查询完毕！");
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
        String queryString = searchSourceBuilder.toString();
        SearchResult searchResult = jsonSearch(queryString, "zhuoqianmingyue_test", "_doc");
        List<SearchResult.Hit<JSONObject, Void>> hits = searchResult.getHits(JSONObject.class);
        Assert.assertNotNull(hits);
        log.info("查询完毕！");
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
        String queryString = searchSourceBuilder.toString();
        SearchResult searchResult = jsonSearch(queryString, "zhuoqianmingyue_test", "_doc");
        List<SearchResult.Hit<JSONObject, Void>> hits = searchResult.getHits(JSONObject.class);
        Assert.assertNotNull(hits);
        log.info("查询完毕！");
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

        String queryString = searchSourceBuilder.toString();
        SearchResult searchResult = jsonSearch(queryString, "zhuoqianmingyue_test", "_doc");
        List<SearchResult.Hit<JSONObject, Void>> hits = searchResult.getHits(JSONObject.class);
        Assert.assertNotNull(hits);
        log.info("查询完毕！");
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

        String queryString = searchSourceBuilder.toString();
        SearchResult searchResult = jsonSearch(queryString, "zhuoqianmingyue_test", "_doc");
        List<SearchResult.Hit<JSONObject, Void>> hits = searchResult.getHits(JSONObject.class);
        Assert.assertNotNull(hits);
        log.info("查询完毕！");
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

        String queryString = searchSourceBuilder.toString();
        SearchResult searchResult = jsonSearch(queryString, "zhuoqianmingyue_test", "_doc");
        List<SearchResult.Hit<JSONObject, Void>> hits = searchResult.getHits(JSONObject.class);
        Assert.assertNotNull(hits);
        log.info("查询完毕！");
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


    /**
     * hit 转换 实体对象
     *
     * @return 查询实体对象
     * @author lijk
     * @date 2021/06/07 14:36
     **/
    public <T> T hitToSource(SearchResult.Hit<JSONObject, Void> hit, Class<T> clazz) {
        JSONObject jsonObject = hit.source;
        return JSON.toJavaObject(jsonObject, clazz);
    }

    /**
     * hit 集合 转换 实体对象集合
     *
     * @return 查询实体对象
     * @author lijk
     * @date 2021/06/07 14:36
     **/
    public <T> List<T> hitListToSourceList(List<SearchResult.Hit<JSONObject, Void>> hits, Class<T> clazz) {
        List<T> list = new ArrayList<>();
        for (SearchResult.Hit<JSONObject, Void> hit : hits) {
            list.add(hitToSource(hit, clazz));
        }
        return list;
    }
}
