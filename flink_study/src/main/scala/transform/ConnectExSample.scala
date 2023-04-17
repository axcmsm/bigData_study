package transform

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.optimizer.operators.MapDescriptor
import org.apache.flink.streaming.api.functions.co.{CoMapFunction, RichCoMapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import sink.Brand_Info
import source.MyParallelSource

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object ConnectExSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1)
    val streamBrandInfo=data.map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })
    streamBrandInfo.print()
    val brand_amount = env.fromCollection(Seq(
      ("1", 200.0),
      ("2", 220.0),
      ("3", 240.0),
      ("4", 400.0),
      ("5", 201.0),
      ("6", 222.0),
      ("7", 500.0)
    ))

    // 合并流的方式有：union，connect，join，协同分组，广播等

    // connect 可以将两个数据类型一样也可以类型不一样 DataStream 连接成一个新的 ConnectedStreams
    //  但是里面的两个流依然是相互独立的，这个方法最大的好处是可以让两个流共享 State 状态,
    //  但这种方式往往结合着广播流或者状态编程来实现
    streamBrandInfo.connect(brand_amount)
      //.map() //促发合并操作
      //.flatMap() //促发合并操作
      // 等转换算子才会促发执行，具体案例可以查看： BroadExample

    env.execute()
  }
}

