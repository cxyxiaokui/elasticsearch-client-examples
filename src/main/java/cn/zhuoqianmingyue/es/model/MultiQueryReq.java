package cn.zhuoqianmingyue.es.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

/**
 * @Author： jkli
 * @Date： 2021/9/12 6:45 下午
 * @Description：
 **/
@ApiModel(value="多字段查询请求参数",description="多字段查询请求参数")
public class MultiQueryReq {

    @ApiModelProperty(value="查询字段",name="fields")
    private List<String> fields;
    @ApiModelProperty(value="查询内容",name="searchContext")
    private String searchContext;

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public String getSearchContext() {
        return searchContext;
    }

    public void setSearchContext(String searchContext) {
        this.searchContext = searchContext;
    }
}
