package cn.zhuoqianmingyue.es.controller;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.zhuoqianmingyue.es.core.BusinessException;
import cn.zhuoqianmingyue.es.core.ControllerSupport;
import cn.zhuoqianmingyue.es.core.Result;
import cn.zhuoqianmingyue.es.model.MockCourse;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author： jkli
 * @Date： 2021/9/12 11:04 上午
 * @Description：
 **/
@Api(tags = "文档新增、Index、删除、查询")
@RestController
@RequestMapping("/es/doc")
public class DocumentController extends ControllerSupport {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @ApiOperation(value = "初始化文档数据", notes = "初始化文档数据")
    @GetMapping("/initDoc")
    public Result initDoc() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest request1 = new IndexRequest("imooc_test");
        request1.id("1");
        List<String> teachers = Arrays.asList("7七月", "千迦");
        Map<Object, Object> build1 = MapUtil.builder()
                .put("id", 1)
                .put("name", "0到1快速掌握Java全栈开发，玩转微信生态")
                .put("price", "348.00")
                .put("studymodel", "easy-level")
                .put("teachers", teachers)
                .put("teachers_count", 2)
                .put("create_date", "2012-09-10 00:00:00")
                .put("description", "实战小程序+公众号+企业微信，一次搞懂SCRM系统").build();
        String jsonString1 = JSON.toJSONString(build1);
        request1.source(jsonString1, XContentType.JSON);
        bulkRequest.add(request1);

        IndexRequest request2 = new IndexRequest("imooc_test");
        request2.id("2");
        Map<Object, Object> build2 = MapUtil.builder()
                .put("id", 2)
                .put("name", "微信小游戏入门与实战 引爆朋友圈")
                .put("price", "99.00")
                .put("studymodel", "middle-level")
                .put("teachers", "千迦")
                .put("teachers_count", 1)
                .put("create_date", "2012-08-01 00:00:00")
                .put("description", "ES5+ES6+小游戏API+开发工具链+经典小游戏开发全过程").build();
        String jsonString2 = JSON.toJSONString(build2);
        request2.source(jsonString2, XContentType.JSON);
        bulkRequest.add(request2);

        IndexRequest request3 = new IndexRequest("imooc_test");
        request3.id("3");
        Map<Object, Object> build3 = MapUtil.builder()
                .put("id", 3)
                .put("name", "three.js-打造微信爆款小游戏跳一跳")
                .put("price", "366.00")
                .put("studymodel", "high-level")
                .put("teachers", "千迦")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "微信小游戏融合three.js+WebGL 打造属于你的爆款3D游戏").build();
        String jsonString3 = JSON.toJSONString(build3);
        request3.source(jsonString3, XContentType.JSON);
        bulkRequest.add(request3);

        IndexRequest request4 = new IndexRequest("imooc_test");
        request4.id("4");
        Map<Object, Object> build4 = MapUtil.builder()
                .put("id", 4)
                .put("name", "实战课ZooKeeper分布式专题与Dubbo微服务入门，成长与加薪必备")
                .put("price", "199.00")
                .put("studymodel", "high-level")
                .put("teachers", "风间影月")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "成长与加薪必备的分布式技能").build();
        String jsonString4 = JSON.toJSONString(build4);
        request4.source(jsonString4, XContentType.JSON);
        bulkRequest.add(request4);

        IndexRequest request5 = new IndexRequest("imooc_test");
        request5.id("5");
        Map<Object, Object> build5 = MapUtil.builder()
                .put("id", 5)
                .put("name", "打造仿猫眼项目 以Dubbo为核心解锁微服务")
                .put("price", "366.00")
                .put("studymodel", "high-level")
                .put("teachers", "Allen")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "Dubbo核心知识点+微服务深度讲解+面试指导，助你过关斩将").build();
        String jsonString5 = JSON.toJSONString(build5);
        request5.source(jsonString5, XContentType.JSON);
        bulkRequest.add(request5);


        IndexRequest request6 = new IndexRequest("imooc_test");
        request6.id("6");
        Map<Object, Object> build6 = MapUtil.builder()
                .put("id", 6)
                .put("name", "免费课2小时实战Apache顶级项目-RPC框架Dubbo分布式服务调度")
                .put("price", "00.00")
                .put("studymodel", "easy-level")
                .put("teachers", "Debug_SteadyJack")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "免费课2小时实战Apache顶级项目-RPC框架Dubbo分布式服务调度").build();
        String jsonString6 = JSON.toJSONString(build6);
        request6.source(jsonString6, XContentType.JSON);
        bulkRequest.add(request6);

        IndexRequest request7 = new IndexRequest("imooc_test");
        request7.id("7");
        Map<Object, Object> build7 = MapUtil.builder()
                .put("id", 7)
                .put("name", "Spring Cloud Alibaba 大型互联网领域多场景最佳实践")
                .put("price", "368.00")
                .put("studymodel", "easy-level")
                .put("teachers", "子牙老师")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "透彻讲解核心组件原理+最佳实践，提升微服务在实际复杂场景中的落地能力").build();
        String jsonString7 = JSON.toJSONString(build7);
        request7.source(jsonString7, XContentType.JSON);
        bulkRequest.add(request7);


        IndexRequest request8 = new IndexRequest("imooc_test");
        request8.id("8");
        Map<Object, Object> build8 = MapUtil.builder()
                .put("id", 8)
                .put("name", "深度解锁SpringCloud主流组件一战解决微服务诸多难题")
                .put("price", "366.00")
                .put("studymodel", "middle-level")
                .put("teachers", "Allen")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "“超硬核” SpringCloud主流组件技术点解剖，“超智囊”微服务开发难题化解").build();
        String jsonString8 = JSON.toJSONString(build8);
        request8.source(jsonString8, XContentType.JSON);
        bulkRequest.add(request8);

        IndexRequest request9 = new IndexRequest("imooc_test");
        request9.id("9");
        Map<Object, Object> build9 = MapUtil.builder()
                .put("id", 9)
                .put("name", "Spring Cloud Alibaba从入门到进阶")
                .put("price", "399.00")
                .put("studymodel", "high-level")
                .put("teachers", "大目")
                .put("teachers_count", 1)
                .put("create_date", "2012-09-01 00:00:00")
                .put("description", "一站式 体系化掌握Alibaba微服务完整生态").build();
        String jsonString9 = JSON.toJSONString(build9);
        request9.source(jsonString9, XContentType.JSON);
        bulkRequest.add(request9);


        IndexRequest request10 = new IndexRequest("imooc_test");
        request10.id("10");
        Map<Object, Object> build10 = MapUtil.builder()
                .put("id", 10)
                .put("name", "Java分布式后台开发 Spring Boot+Kafka+HBase")
                .put("price", "299.00")
                .put("studymodel", "middle-level")
                .put("teachers", "张勤一")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "高可用后台+企业级架构").build();
        String jsonString10 = JSON.toJSONString(build10);
        request10.source(jsonString10, XContentType.JSON);
        bulkRequest.add(request10);


        IndexRequest request11 = new IndexRequest("imooc_test");
        request11.id("11");
        Map<Object, Object> build11 = MapUtil.builder()
                .put("id", 11)
                .put("name", "Spring Security + OAuth2 精讲 多场景打造企业级认证与授权")
                .put("price", "348.00")
                .put("studymodel", "middle-level")
                .put("teachers", "接灰的电子产品")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "一站式掌握主流安全框架与行业解决方案，从容应对各种安全难题").build();
        String jsonString11 = JSON.toJSONString(build11);
        request11.source(jsonString11, XContentType.JSON);
        bulkRequest.add(request11);

        IndexRequest request12 = new IndexRequest("imooc_test");
        request12.id("12");
        Map<Object, Object> build12 = MapUtil.builder()
                .put("id", 12)
                .put("name", "Spring Cloud分布式微服务实战 打造大型自媒体3大业务平台")
                .put("price", "468.00")
                .put("studymodel", "middle-level")
                .put("teachers", "风间影月")
                .put("teachers_count", 1)
                .put("create_date", "2011-09-01 00:00:00")
                .put("description", "分布式/前后端分离/项目分层聚合 养成应对复杂业务的综合技术能力").build();
        String jsonString12 = JSON.toJSONString(build12);
        request12.source(jsonString12, XContentType.JSON);
        bulkRequest.add(request12);

        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return sucess(bulk.status());
    }

    @ApiOperation(value = "新增文档", notes = "新增文档")
    @PostMapping("/create")
    public Result addDoc(@RequestBody MockCourse mockCourse) throws IOException {
        IndexRequest request = new IndexRequest("imooc_test");
        request.id(mockCourse.getId());

        String jsonString = JSONObject.toJSONString(mockCourse);
        request.source(jsonString, XContentType.JSON);
        request.create(true);
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        RestStatus status = index.status();
        return sucess(RestStatus.CREATED.equals(status) || RestStatus.OK.equals(status));
    }

    @ApiOperation(value = "索引（Index）", notes = "索引（Index）")
    @PostMapping("/index")
    public Result index(@RequestBody MockCourse mockCourse) throws IOException {
        IndexRequest request = new IndexRequest("imooc_test");
        request.id(mockCourse.getId());

        String jsonString = JSONObject.toJSONString(mockCourse);
        request.source(jsonString, XContentType.JSON);
        request.create(false);
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        RestStatus status = index.status();
        return sucess(RestStatus.CREATED.equals(status) || RestStatus.OK.equals(status));
    }

    @ApiOperation(value = "根据id查询文档信息", notes = "根据id查询文档信息")
    @GetMapping("/{id}")
    @ApiImplicitParam(paramType = "path", name = "id", value = "文档id", required = true, dataType = "string")
    public Result getById(@PathVariable String id) throws IOException {
        GetRequest getRequest = new GetRequest("imooc_test", id);
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        return sucess(getResponse);
    }

    @ApiOperation(value = "更新文档", notes = "更新文档")
    @PutMapping("/{id}")
    public Result update(@RequestBody MockCourse mockCourse) throws IOException {
        if (StrUtil.isBlank(mockCourse.getId())) {
            throw new BusinessException("id 不能为空！");
        }
        String jsonString = JSONObject.toJSONString(mockCourse);
        UpdateRequest updateRequest = new UpdateRequest("imooc_test", mockCourse.getId());
        updateRequest.doc(jsonString, XContentType.JSON);

        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        RestStatus status = update.status();
        return sucess(status);
    }


    @ApiOperation(value = "查询所有文档", notes = "查询所有文档")
    @GetMapping()
    public Result queryAll() throws IOException {
        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("imooc_test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10000);
        //设置要显示的字段
        //searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "create_date"}, new String[]{});
        searchRequest.source(searchSourceBuilder);

        //ES发起http请求
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        return sucess(hits);
    }

    @ApiOperation(value = "根据Id删除文档", notes = "根据Id删除文档")
    @DeleteMapping("/{id}")
    public Result deleteDoc(@PathVariable String id) throws IOException {
        DeleteRequest request = new DeleteRequest("imooc_test");
        request.id(id);
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        RestStatus status = delete.status();
        return sucess(status);
    }
}
