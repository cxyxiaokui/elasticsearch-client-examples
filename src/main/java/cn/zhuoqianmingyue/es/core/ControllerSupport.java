package cn.zhuoqianmingyue.es.core;

/**
 * Controler 扩展功能封装
 *
 * @Author zhuoqianmingyue
 * @Date 2020/4/5 11:39 上午
 * @Description：对 Contoller 经常使用的逻辑进行抽取
 **/
public class ControllerSupport<T> {

    public Result<T> sucess(){
        return ResultGenerator.genSuccessResult();
    }

    public Result<T> sucess(T object){
        return  ResultGenerator.genSuccessResult(object);
    }
}
