package cn.zhuoqianmingyue.es.controller;

/**
 * @Author： jkli
 * @Date： 2021/9/12 10:20 上午
 * @Description：
 **/

import cn.zhuoqianmingyue.es.core.ControllerSupport;
import cn.zhuoqianmingyue.es.core.Result;
import com.alibaba.fastjson.JSON;
import com.github.xiaoymin.knife4j.annotations.ApiOperationSupport;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Api(tags ="索引库新增、删除、查询")
@RestController
@RequestMapping("/es/index")
public class IndexController extends ControllerSupport {

    private static final Logger log = LoggerFactory.getLogger(IndexController.class);

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @ApiOperation(value = "新增索引", notes = "新增索引")
    @ApiOperationSupport(author = "zhuoqianmingyue")
    @PostMapping
    @ApiImplicitParams({
            @ApiImplicitParam(name="mapping",value="新增文档mapping",dataType="string", required = true, paramType = "form",example="{\n" +
                    "    \"properties\": {\n" +
                    "      \"id\":{\n" +
                    "      \t\"type\":\"long\"\n" +
                    "      },\n" +
                    "      \"description\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"analyzer\": \"ik_max_word\",\n" +
                    "        \"search_analyzer\": \"ik_smart\"\n" +
                    "      },\n" +
                    "      \"name\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"analyzer\": \"ik_max_word\",\n" +
                    "        \"search_analyzer\": \"ik_smart\",\n" +
                    "        \"fields\": {\n" +
                    "          \"keyword\" : {\n" +
                    "            \"type\" : \"keyword\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"pic\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"index\": false\n" +
                    "      },\n" +
                    "      \"price\": {\n" +
                    "        \"type\": \"double\"\n" +
                    "      },\n" +
                    "      \"studymodel\": {\n" +
                    "        \"type\": \"keyword\"\n" +
                    "      },\n" +
                    "      \"teachers\" : {\n" +
                    "        \"type\" : \"text\",\n" +
                    "        \"fields\" : {\n" +
                    "          \"keyword\" : {\n" +
                    "            \"type\" : \"keyword\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"teachers_count\" : {\n" +
                    "        \"type\" : \"integer\"\n" +
                    "      },\n" +
                    "      \"create_date\": {\n" +
                    "        \"type\": \"date\",\n" +
                    "        \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||yyyy-MM-dd'T'HH:mm:ss+0800\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "\t}"),
            @ApiImplicitParam(name="shards",value="新增文档Setting 主分片数",dataType="string", paramType = "form", example="1"),
            @ApiImplicitParam(name="replicas",value="新增文档Setting 副本数量",dataType="string", paramType = "form", example="1")})
    public Result addIndex(@RequestParam("mapping") String mapping,
                           @RequestParam("shards") String shards,
                           @RequestParam("replicas") String replicas) throws IOException {
        //添加索引库
        CreateIndexRequest request = new CreateIndexRequest("imooc_test");
        //设置分片 和副本
        request.settings(Settings.builder().put("number_of_shards", shards).put("number_of_replicas", replicas));
        //设置Mapping
        // 创建映射
        request.mapping(mapping, XContentType.JSON);

        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = createIndexResponse.isAcknowledged();
        return sucess(acknowledged);
    }

    @ApiOperation(value = "删除索引", notes = "删除索引")
    @DeleteMapping("/{indexName}")
    @ApiImplicitParam(paramType= "path", name = "indexName", value = "索引库名称", required = true, dataType = "string", example="imooc_test")
    public Result delete(@PathVariable String indexName) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        boolean acknowledged = delete.isAcknowledged();
        return sucess(acknowledged);
    }

    @ApiOperation(value = "查询索引信息", notes = "查询索引信息")
    @GetMapping("/{indexName}")
    @ApiImplicitParam(paramType= "path", name = "indexName", value = "索引库名称", required = true, dataType = "string", example="imooc_test")
    public Result getById(@PathVariable String indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(request, RequestOptions.DEFAULT);

        String[] indices = getIndexResponse.getIndices();
        log.info("索引名称 {}", JSON.toJSONString(indices));
        Map<String, MappingMetaData> mappings = getIndexResponse.getMappings();
        log.info("索引 Mapping 信息 {}", JSON.toJSONString(mappings));
        Map<String, Settings> settings = getIndexResponse.getSettings();
        Map<String, Object> map = new HashMap<>(16);
        map.put("mappings", JSON.toJSONString(mappings));
        //map.put("settings", JSON.toJSONString(settings));
        return sucess(mappings);
    }
}
