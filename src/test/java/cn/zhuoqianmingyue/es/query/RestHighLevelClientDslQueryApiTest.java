package cn.zhuoqianmingyue.es.query;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.zhuoqianmingyue.es.Application;
import cn.zhuoqianmingyue.es.index.RestHighLevelClientIndexApiTest;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.Scroll;
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
import java.util.Arrays;
import java.util.List;
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
     *{
     *   "query": {
     *     "match_all": {
     *       "boost": 1.0
     *     }
     *   },
     *   "_source": {
     *     "includes": [
     *       "name",
     *       "studymodel",
     *       "price",
     *       "create_date"
     *     ],
     *     "excludes": [ ]
     *   },
     *   "sort": [
     *     {
     *       "_id": {
     *         "order": "desc"
     *       }
     *     }
     *   ]
     * }
     * @throws IOException
     */
    @Test
    public void matchAllQueryTest() throws IOException {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.sort("_id", SortOrder.DESC);
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
     * {
     *   "from": 2,
     *   "size": 2,
     *   "query": {
     *     "match_all": {
     *       "boost": 1.0
     *     }
     *   },
     *   "_source": {
     *     "includes": [
     *       "name",
     *       "studymodel",
     *       "price",
     *       "create_date"
     *     ],
     *     "excludes": [ ]
     *   },
     *   "sort": [
     *     {
     *       "_id": {
     *         "order": "desc"
     *       }
     *     }
     *   ]
     * }
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
        IndexRequest request1 = new IndexRequest("imooc_test");
        request1.id("1");
        List<String> teachers = Arrays.asList("7七月", "千迦");
        Map<Object, Object> build1 = MapUtil.builder()
                .put("name", "0到1快速掌握Java全栈开发，玩转微信生态")
                .put("price", "348.00")
                .put("studymodel", "easy-level")
                .put("teachers", teachers)
                .put("create_date", "2012-09-10 00:00:00")
                .put("description", "实战小程序+公众号+企业微信，一次搞懂SCRM系统").build();
        String jsonString1 = JSON.toJSONString(build1);
        request1.source(jsonString1, XContentType.JSON);
        bulkRequest.add(request1);

        IndexRequest request2 = new IndexRequest("imooc_test");
        request2.id("2");
        Map<Object, Object> build2 = MapUtil.builder()
                .put("name", "微信小游戏入门与实战 引爆朋友圈")
                .put("price", "99.00")
                .put("studymodel", "middle-level")
                .put("teachers", "千迦")
                .put("create_date", "2012-08-01 00:00:00")
                .put("description", "ES5+ES6+小游戏API+开发工具链+经典小游戏开发全过程").build();
        String jsonString2 = JSON.toJSONString(build2);
        request2.source(jsonString2, XContentType.JSON);
        bulkRequest.add(request2);

        IndexRequest request3 = new IndexRequest("imooc_test");
        request3.id("3");
        Map<Object, Object> build3 = MapUtil.builder()
                .put("name", "three.js-打造微信爆款小游戏跳一跳")
                .put("price", "366.00")
                .put("studymodel", "high-level")
                .put("teachers", "千迦")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "微信小游戏融合three.js+WebGL 打造属于你的爆款3D游戏").build();
        String jsonString3 = JSON.toJSONString(build3);
        request3.source(jsonString3, XContentType.JSON);
        bulkRequest.add(request3);

        IndexRequest request4 = new IndexRequest("imooc_test");
        request4.id("4");
        Map<Object, Object> build4 = MapUtil.builder()
                .put("name", "实战课ZooKeeper分布式专题与Dubbo微服务入门，成长与加薪必备")
                .put("price", "199.00")
                .put("studymodel", "high-level")
                .put("teachers", "风间影月")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "成长与加薪必备的分布式技能").build();
        String jsonString4 = JSON.toJSONString(build4);
        request4.source(jsonString4, XContentType.JSON);
        bulkRequest.add(request4);

        IndexRequest request5 = new IndexRequest("imooc_test");
        request5.id("5");
        Map<Object, Object> build5 = MapUtil.builder()
                .put("name", "打造仿猫眼项目 以Dubbo为核心解锁微服务")
                .put("price", "366.00")
                .put("studymodel", "high-level")
                .put("teachers", "Allen")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "Dubbo核心知识点+微服务深度讲解+面试指导，助你过关斩将").build();
        String jsonString5 = JSON.toJSONString(build5);
        request5.source(jsonString5, XContentType.JSON);
        bulkRequest.add(request5);


        IndexRequest request6 = new IndexRequest("imooc_test");
        request6.id("6");
        Map<Object, Object> build6 = MapUtil.builder()
                .put("name", "免费课2小时实战Apache顶级项目-RPC框架Dubbo分布式服务调度")
                .put("price", "00.00")
                .put("studymodel", "easy-level")
                .put("teachers", "Debug_SteadyJack")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "免费课2小时实战Apache顶级项目-RPC框架Dubbo分布式服务调度").build();
        String jsonString6 = JSON.toJSONString(build6);
        request6.source(jsonString6, XContentType.JSON);
        bulkRequest.add(request6);

        IndexRequest request7 = new IndexRequest("imooc_test");
        request7.id("7");
        Map<Object, Object> build7 = MapUtil.builder()
                .put("name", "Spring Cloud Alibaba 大型互联网领域多场景最佳实践")
                .put("price", "368.00")
                .put("studymodel", "easy-level")
                .put("teachers", "子牙老师")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "透彻讲解核心组件原理+最佳实践，提升微服务在实际复杂场景中的落地能力").build();
        String jsonString7 = JSON.toJSONString(build7);
        request7.source(jsonString7, XContentType.JSON);
        bulkRequest.add(request7);


        IndexRequest request8 = new IndexRequest("imooc_test");
        request8.id("8");
        Map<Object, Object> build8 = MapUtil.builder()
                .put("name", "深度解锁SpringCloud主流组件一战解决微服务诸多难题")
                .put("price", "366.00")
                .put("studymodel", "middle-level")
                .put("teachers", "Allen")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "“超硬核” SpringCloud主流组件技术点解剖，“超智囊”微服务开发难题化解").build();
        String jsonString8 = JSON.toJSONString(build8);
        request8.source(jsonString8, XContentType.JSON);
        bulkRequest.add(request8);

        IndexRequest request9 = new IndexRequest("imooc_test");
        request9.id("9");
        Map<Object, Object> build9 = MapUtil.builder()
                .put("name", "Spring Cloud Alibaba从入门到进阶")
                .put("price", "399.00")
                .put("studymodel", "high-level")
                .put("teachers", "大目")
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "一站式 体系化掌握Alibaba微服务完整生态").build();
        String jsonString9 = JSON.toJSONString(build9);
        request9.source(jsonString9, XContentType.JSON);
        bulkRequest.add(request9);


        IndexRequest request10 = new IndexRequest("imooc_test");
        request10.id("10");
        Map<Object, Object> build10 = MapUtil.builder()
                .put("name", "Java分布式后台开发 Spring Boot+Kafka+HBase")
                .put("price", "299.00")
                .put("studymodel", "middle-level")
                .put("teachers", "张勤一")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "高可用后台+企业级架构").build();
        String jsonString10 = JSON.toJSONString(build10);
        request10.source(jsonString10, XContentType.JSON);
        bulkRequest.add(request10);


        IndexRequest request11 = new IndexRequest("imooc_test");
        request11.id("11");
        Map<Object, Object> build11 = MapUtil.builder()
                .put("name", "Spring Security + OAuth2 精讲 多场景打造企业级认证与授权")
                .put("price", "348.00")
                .put("studymodel", "middle-level")
                .put("teachers", "接灰的电子产品")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "一站式掌握主流安全框架与行业解决方案，从容应对各种安全难题").build();
        String jsonString11 = JSON.toJSONString(build11);
        request11.source(jsonString11, XContentType.JSON);
        bulkRequest.add(request11);

        IndexRequest request12 = new IndexRequest("imooc_test");
        request12.id("12");
        Map<Object, Object> build12 = MapUtil.builder()
                .put("name", "Spring Cloud分布式微服务实战 打造大型自媒体3大业务平台")
                .put("price", "468.00")
                .put("studymodel", "middle-level")
                .put("teachers", "风间影月")
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "分布式/前后端分离/项目分层聚合 养成应对复杂业务的综合技术能力").build();
        String jsonString12 = JSON.toJSONString(build12);
        request12.source(jsonString12, XContentType.JSON);
        bulkRequest.add(request12);

        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    /**
     * searchAfter
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
     * @throws IOException
     */
    @Test
    public void searchAfterQueryTest() throws IOException {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(2);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.sort("_id", SortOrder.DESC);
        //设置要显示的字段
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "create_date"}, new String[]{});
        searchRequest.source(searchSourceBuilder);

        //ES发起http请求
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hitsArray = hits.getHits();
        while (ArrayUtil.isNotEmpty(hitsArray)){
            for (SearchHit hit : hitsArray) {
                log.info(hit.getSourceAsString());
            }
            Object[] sortValues = hitsArray[hitsArray.length - 1].getSortValues();
            SearchHits searchHits = searchAfter(sortValues);
            hitsArray = searchHits.getHits();
        }
    }

    /**
     * searchAfter 的查询
     * @param sortValues 最后一个排序的值
     * @return SearchHits
     * @throws IOException
     */
    public SearchHits searchAfter(Object[] sortValues) throws IOException {
        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("zhuoqianmingyue_test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(2);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.sort("_id", SortOrder.DESC);
        searchSourceBuilder.searchAfter(sortValues);
        //设置要显示的字段
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "create_date"}, new String[]{});
        searchRequest.source(searchSourceBuilder);

        //ES发起http请求
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse.getHits();
    }

    @Test
    public void scrollQueryTest() throws IOException {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("abc","zhuoqianmingyue_test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(2);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置要显示的字段
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "create_date"}, new String[]{});
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMillis(500L));
        //ES发起http请求
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] hitsArray = hits.getHits();
        while (ArrayUtil.isNotEmpty(hitsArray)){
            for (SearchHit hit : hitsArray) {
                log.info(hit.getSourceAsString());
            }

            searchResponse = scroll(searchResponse.getScrollId());
            hitsArray = searchResponse.getHits().getHits();
        }
    }

    /**
     * scroll 的查询
     * @param scrollId 最后一个排序的值
     * @return SearchHits
     * @throws IOException
     */
    public SearchResponse scroll(String scrollId) throws IOException {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueMillis(500L));

        SearchResponse searchScrollResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);

        return searchScrollResponse;
    }
}
