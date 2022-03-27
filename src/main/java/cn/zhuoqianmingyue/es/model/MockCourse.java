package cn.zhuoqianmingyue.es.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Date;
import java.util.List;

/**
 * @Author： jkli
 * @Date： 2021/9/12 11:07 上午
 * @Description：
 **/
@ApiModel(value="慕课课程对象",description="慕课课程对象")
public class MockCourse {
    @ApiModelProperty(value="id",name="id")
    private String id;
    @ApiModelProperty(value="课程描述",name="description")
    private String description;
    @ApiModelProperty(value="课程名称",name="name")
    private String name;
    @ApiModelProperty(value="课程封面",name="pic")
    private String pic;
    @ApiModelProperty(value="课程价格",name="price")
    private Double price;
    @ApiModelProperty(value="课程level",name="studymodel")
    private String studymodel;
    @ApiModelProperty(value="课程创建日期",name="create_date")
    private Date create_date;
    @ApiModelProperty(value="课程老师",name="teachersStr")
    private List<String> teachers;

    public List<String> getTeachers() {
        return teachers;
    }

    public void setTeachers(List<String> teachers) {
        this.teachers = teachers;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPic() {
        return pic;
    }

    public void setPic(String pic) {
        this.pic = pic;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getStudymodel() {
        return studymodel;
    }

    public void setStudymodel(String studymodel) {
        this.studymodel = studymodel;
    }

    public Date getCreate_date() {
        return create_date;
    }

    public void setCreate_date(Date create_date) {
        this.create_date = create_date;
    }
}
