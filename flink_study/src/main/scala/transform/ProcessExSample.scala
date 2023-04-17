package transform

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import sink.Brand_Info
import source.MyParallelSource

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object ProcessExSample {
  //process function 相对于前文所述的 map、flatmap、filter 算子来说，最大的区别是其让开发人员对数据
  //的 处 理 逻 辑 拥 有 更 大 的 自 由 度 ； 同 时 ， ProcessFunction 继 承 了 RichFunction ， 因 而 具 备 了
  //getRuntimeContext() ，open() ，close()等方法
  def main(args: Array[String]): Unit = {
    //flink 提供了大量不同类型的 process function，让其针对不同的 datastream 拥有更具针对性的功能
    //例如：
    // ProcessFunction （普通 DataStream 上调 process 时）
    // KeyedProcessFunction （KeyedStream 上调 process 时）
    // ProcessWindowFunction（WindowedStream 上调 process 时）
    // BroadcastProcessFunction （BroadCastConnectedStreams  上调 process 时)
    // 等等...
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val streamBrandInfo = env.addSource(new MyParallelSource).setParallelism(1).map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })

    //TODO （普通 DataStream 上调 process 时）
    streamBrandInfo.process(new ProcessFunction[Brand_Info,String] {
      override def open(parameters: Configuration): Unit = {
        //getRuntimeContext.getState()//状态编程
      }
      override def processElement(i: Brand_Info, context: ProcessFunction[Brand_Info, String]#Context, collector: Collector[String]): Unit = {
        //context.timerService().registerEventTimeTimer()//注册定时器
      }
      override def close(): Unit = {
      }
    })

    //TODO  （KeyedStream 上调 process 时）
    streamBrandInfo.keyBy(_.brand_id).process(new KeyedProcessFunction[String,Brand_Info,String] {
      override def open(parameters: Configuration): Unit = {
        //getRuntimeContext.getState()//状态编程
      }
      override def processElement(i: Brand_Info, context: KeyedProcessFunction[String, Brand_Info, String]#Context, collector: Collector[String]): Unit = {
        //context.timerService().registerEventTimeTimer()//注册定时器
      }
      override def close(): Unit = {
      }
//      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Brand_Info, String]#OnTimerContext, out: Collector[String]): Unit = {
//        //定时器触发
//      }
    })

    env.execute()
  }
}
