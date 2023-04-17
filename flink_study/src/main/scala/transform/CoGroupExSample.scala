package transform

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.lang

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object CoGroupExSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.fromCollection(Seq(
      ("1", "张三"),
      ("2", "里斯"),
      ("3", "王五"),
      ("4", "老六"),
      ("5", "小七"),
      ("6", "老八"),
      ("7", "九子")
    ))
    val stream2 = env.fromCollection(Seq(
      ("1", 200.0),
      ("2", 220.0),
      ("3", 240.0),
      ("4", 400.0),
      ("5", 201.0),
      ("6", 222.0),
      ("7", 500.0)
    ))

    //coGroup 本质上是上述 join 算子的底层算子；功能类似
    stream1.coGroup(stream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply(new CoGroupFunction[(String,String),(String,Double),(String,String,Double)] {
        override def coGroup(iterable: lang.Iterable[(String, String)], iterable1: lang.Iterable[(String, Double)], collector: Collector[(String, String, Double)]): Unit = {
          //TODO 处理逻辑...

        }
      })

    env.execute()
  }
}
