package transform

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import sink.Brand_Info
import source.MyParallelSource

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 分流案例
 * @author 须贺
 * @version 2023/4/17 
 */
object SplitExSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1)
    val streamBrandInfo=data.map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })
    streamBrandInfo.print()
    //方式：使用 filter 和 测输出流 都可以实现相同的效果
    //品牌信息表：品牌ID，品牌名称，联系电话，'品牌状态,0禁用,1启用'
    //数据格式： 1,华为,15138641134,1
    
    //例如：将数据为品牌信息中，为禁用和启用的数据进行分流操作
    //filter的方式
    streamBrandInfo.filter(_.brand_status.equals("1")).print("启用")
    streamBrandInfo.filter(_.brand_status.equals("0")).print("禁用")
    
    
    //侧输出流
    val disableTag = new OutputTag[Brand_Info]("disable")
    val res = streamBrandInfo.process(new ProcessFunction[Brand_Info, Brand_Info] {
      override def processElement(i: Brand_Info, context: ProcessFunction[Brand_Info, Brand_Info]#Context, collector: Collector[Brand_Info]): Unit = {
        if (i.brand_status.equals("0")) {
          context.output(disableTag, i)
        }
        collector.collect(i)
      }
    })
    res.print("启用_tag")
    res.getSideOutput(disableTag).print("禁用_tag")


  }
}
