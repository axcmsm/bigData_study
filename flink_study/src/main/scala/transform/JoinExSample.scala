package transform

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object JoinExSample {
  def main(args: Array[String]): Unit = {
    //用于关联两个流（类似于 sql 中 join），需要指定 join 的条件；需要在窗口中进行关联后的逻辑计算
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

    //处理模板格式：
    //stream.join(otherStream)
    //.where(<KeySelector>)
    //.equalTo(<KeySelector>)
    //.window(<WindowAssigner>)
    //.apply(<JoinFunction>)
    stream1.join(stream2)
      .where(_._1)
      .equalTo(_._1)
      //tumbling window 滚动窗口
      //sliding window 滑动窗口
      //session window 会话窗口
      //以及时间的选择：处理时间和事件事件  Processing，EventTime
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //关联的选择，内连接，外连接
      .apply(new JoinFunction[(String,String),(String,Double),(String,String,Double)] {
        override def join(in1: (String, String), in2: (String, Double)): (String, String, Double) = {
          //TODO 处理逻辑
          ("","",0)
        }
      })





  }
}
