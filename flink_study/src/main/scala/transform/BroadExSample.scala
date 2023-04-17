package transform

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17
 */
object BroadExSample {
  def main(args: Array[String]): Unit = {
    //广播流操作
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1 = env.fromCollection(Seq(
      ("1", "张三"),
      ("2", "里斯"),
      ("3", "王五"),
      ("4", "老六"),
      ("5", "小七"),
      ("6", "老八"),
      ("7", "九子")
    ))
    val stream2 = env.socketTextStream("master",7777).map(iter=>{
      val arr = iter.trim.split(",")
      (arr(0),arr(1).toDouble)
    })

    //将stream1进行广播操作
    val broadMapState = new MapStateDescriptor[String, String]("broadMapState_", classOf[String], classOf[String])
    val broadStream = stream1.broadcast(broadMapState)

    //数据流连接到广播流中进行操作
    val res = stream2.connect(broadStream).process(new BroadcastProcessFunction[(String, Double), (String, String), (String, String, Double)] {
      override def processBroadcastElement(in2: (String, String), context: BroadcastProcessFunction[(String, Double), (String, String), (String, String, Double)]#Context, collector: Collector[(String, String, Double)]): Unit = {
        //添加广播数据
        val broadStateValue = context.getBroadcastState(broadMapState)
        broadStateValue.put(in2._1, in2._2)
        println(in2)
      }
      override def processElement(in1: (String, Double), readOnlyContext: BroadcastProcessFunction[(String, Double), (String, String), (String, String, Double)]#ReadOnlyContext, collector: Collector[(String, String, Double)]): Unit = {
        //处理数据流
        val readBroadValue = readOnlyContext.getBroadcastState(broadMapState)
        val name = readBroadValue.get(in1._1)
        collector.collect((in1._1, name, in1._2))
      }
    })
    res.print()

    env.execute()
    /**
    //测试：nc -lk 7777
    1,200.0
    2,300.0
    5,400.0
    4,600.0
     */
  }
}
