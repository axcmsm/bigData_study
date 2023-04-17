package transform

import org.apache.flink.api.scala.createTypeInformation
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
object UnionExSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val streamBrandInfo1 = env.addSource(new MyParallelSource).setParallelism(1).map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })

    val streamBrandInfo2 = env.addSource(new MyParallelSource).setParallelism(1).map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })

    val streamBrandInfo3 = env.addSource(new MyParallelSource).setParallelism(1).map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })


    // 合并流的方式有：union，connect，join，协同分组，广播等

    // union 可以将两个或者多个数据类型一致的 DataStream 合并成一个 DataStream.
    streamBrandInfo1.union(streamBrandInfo2).union(streamBrandInfo3).print("data")

    env.execute()
  }
}
