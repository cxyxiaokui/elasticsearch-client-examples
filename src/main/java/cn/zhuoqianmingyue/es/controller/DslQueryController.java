package cn.zhuoqianmingyue.es.controller;

import cn.hutool.core.util.StrUtil;
import cn.zhuoqianmingyue.es.core.ControllerSupport;
import cn.zhuoqianmingyue.es.core.Result;
import cn.zhuoqianmingyue.es.model.MultiQueryReq;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * @Author： jkli
 * @Date： 2021/9/12 4:53 下午
 * @Description：
 **/
@Api(tags = "DSL 查询")
@RestController
@RequestMapping("/es/doc")
public class DslQueryController extends ControllerSupport {

    @Autowired
    private RestHighLevelClient restHighLevelClient;
    @ApiOperation(value = "精确查询 termQuery", notes = "精确查询 termQuery")
    @GetMapping("/termQuery")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "fieldName", value = "搜索字段", dataType = "string", paramType = "form"),
            @ApiImplicitParam(name = "searchContext", value = "搜索内容", dataType = "string", paramType = "form"),
            @ApiImplicitParam(name = "isWithKeyWord", value = "是否已keyword 类型进行查询（如果字段本身是keyword 则不用设置）", dataType = "string", paramType = "form")})
    public Result termQuery(@RequestParam("fieldName") String fieldName,
                             @RequestParam("searchContext") String searchContext,
                             @RequestParam(value = "", defaultValue = "false") boolean isWithKeyWord) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(fieldName, searchContext);
        if (isWithKeyWord) {
            termQueryBuilder = QueryBuilders.termQuery(fieldName+".keyword", searchContext);
        }

        searchSourceBuilder.query(termQueryBuilder);
        SearchRequest searchRequest = new SearchRequest("imooc_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        return sucess(hits);
    }

    @ApiOperation(value = "单个字段全文检索查询", notes = "单个字段全文检索查询")
    @GetMapping("/matchQuery")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "fieldName", value = "搜索字段", dataType = "string", paramType = "form"),
            @ApiImplicitParam(name = "searchContext", value = "搜索内容", dataType = "string", paramType = "form"),
            @ApiImplicitParam(name = "operator", value = "查询表达式使用AND 表示搜索内容要同时出现否则就是 OR", dataType = "string", paramType = "form")})
    public Result matchQuery(@RequestParam("fieldName") String fieldName,
                             @RequestParam("searchContext") String searchContext,
                             @RequestParam(value = "operator", defaultValue = "") String operator) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(fieldName, searchContext);
        if (!StrUtil.isBlank(operator)) {
            matchQueryBuilder = matchQueryBuilder.operator(Operator.AND);
        }

        searchSourceBuilder.query(matchQueryBuilder);
        SearchRequest searchRequest = new SearchRequest("imooc_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        return sucess(hits);
    }

    @ApiOperation(value = "多个字段全文检索查询", notes = "多个字段全文检索查询")
    @PostMapping("/multiQueryQuery")
    public Result multiQueryQuery(@RequestBody MultiQueryReq multiQueryReq) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String[] fieldNames = multiQueryReq.getFields().toArray(new String[multiQueryReq.getFields().size()]);
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(multiQueryReq.getSearchContext(), fieldNames));

        SearchRequest searchRequest = new SearchRequest("imooc_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        return sucess(hits);
    }

    @ApiOperation(value = "短语全文查询", notes = "短语全文查询")
    @GetMapping("/matchPhraseQuery")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "fieldName", value = "搜索字段", dataType = "string", paramType = "form"),
            @ApiImplicitParam(name = "searchContext", value = "搜索内容", dataType = "string", paramType = "form"),
            @ApiImplicitParam(name = "slop", value = "中间间隔多个token 位置", dataType = "integer", paramType = "form")})
    public Result matchPhraseQuery(@RequestParam("fieldName") String fieldName,
                                   @RequestParam("searchContext") String searchContext,
                                   @RequestParam("slop") int slop) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery(fieldName, searchContext).slop(slop);
        searchSourceBuilder.query(matchPhraseQueryBuilder);
        SearchRequest searchRequest = new SearchRequest("imooc_test");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        return sucess(hits);
    }



    @ApiOperation(value = "分页加排序", notes = "分页加排序")
    @GetMapping("/queryPageAndSort")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "page", value = "当前页数", dataType = "int", paramType = "form"),
            @ApiImplicitParam(name = "size", value = "每页显示条数", dataType = "int", paramType = "form")})
    public Result queryPageAndSort(@RequestParam("page") int page,
                                   @RequestParam("size") int size) throws IOException {
        // 搜索请求对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //设置要显示的字段
        //searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "create_date"}, new String[]{});

        //设置分页的from to
        int index = (page - 1) * size;
        searchSourceBuilder.from(index);
        searchSourceBuilder.size(size);
        searchSourceBuilder.sort("id", SortOrder.ASC);

        SearchRequest searchRequest = new SearchRequest("imooc_test");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        return sucess(hits);
    }
}
