package transform

import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import source.MyParallelSource

import scala.util.Random

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 基本转换
 * @author 须贺
 * @version 2023/4/15 
 */
object TransformExSample {

  def main(args: Array[String]): Unit = {
    //在 Flink 中，常用的转换算子有 map、flatMap、filter、keyBy、reduce、window、join、union 等。

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1) //读取自定义数据源
    //品牌信息表：品牌ID，品牌名称，联系电话，'品牌状态,0禁用,1启用'
    //数据格式： 1,华为,15138641134,1
    //TODO 需求一： 使用Brand_Event对象封装，其中数据品牌名称要求【】括起来，并且添加一个1到100的随机数作为金额，要求输出格式：Brand_Event(1,【华为】,15138641134,1)
    val res1 = data.map(iter => {
      val arr = iter.split(",")
      val randomAmount = Random.nextInt(100) + 1
      Brand_Event(arr(0),"【"+arr(1)+"】",arr(2),arr(3),randomAmount)
    })
    //res1.print("res1")

    //TODO 需求二：在需求一的基础上，过滤掉0禁用的品牌信息
    val res2 = res1.filter(_.brand_status.equals("1"))
    //res2.print("res2")

    //TODO 需求三：前面俩步骤合并成一步完成。
    val res3 = data.flatMap(new FlatMapFunction[String, Brand_Event] {
      override def flatMap(t: String, collector: Collector[Brand_Event]): Unit = {
        val arr = t.split(",")
        val randomAmount = Random.nextInt(100) + 1
        val event = Brand_Event(arr(0), "【" + arr(1) + "】", arr(2), arr(3),randomAmount)
        if (event.brand_status.equals("1")) {
          collector.collect(event)
        }
      }
    })
    //res3.print("res3")


    //sum、min、minBy、max、maxBy 这些为滚动聚合算子，需要在keyBy后才能调用
    //TODO 需求四：在需求三的基础上求出每个品牌出现的个数
    val res4 = res3.map(iter => (iter.brand_name, 1)).keyBy(_._1).sum(1)
    //res4.print("res4")

    //TODO 需求五：在需求三的基础上，求出数据的金额最大值(包含 整条数据)  和  数据金额的最小值(输出值即可)
    res3.keyBy(_=>true).maxBy("amount")
      //.print("max_amount_data") //By 更新整条数据
    res3.keyBy(_=>true).min("amount")
      //.print("min_amount")   //只更新对应的列
    //minBy max 也是一样的


    //TODO 需求六：在需求三的基础上，使用 reduce 实现每个品牌的总金额和,且状态全部改为禁用
    val res6 = res3.keyBy(_.brand_id).reduce(new ReduceFunction[Brand_Event] {
      override def reduce(t: Brand_Event, t1: Brand_Event): Brand_Event = {
        val sum = t.amount + t1.amount
        Brand_Event(t.brand_id, t.brand_name, t.telephone, "0", sum) //sum 的效果
        //Brand_Event(t1.brand_id, t1.brand_name, t1.telephone, "0", sum) //sumBy 的效果
      }
    })
    res6.print("res6")



    env.execute("TransformExample")
  }
}

case class Brand_Event(
                        brand_id: String,
                        brand_name: String,
                        telephone: String,
                        brand_status: String,
                        amount:Double
                      )