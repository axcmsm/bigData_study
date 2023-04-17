package window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object NonKeyedWindowsExSample {
  def main(args: Array[String]): Unit = {
    /**
     *NonKeyedWindows 模板
    stream
    .windowAll(...) <- required: "assigner"
    [.trigger(...)] <- optional: "trigger" (else default trigger)
    [.evictor(...)] <- optional: "evictor" (else no evictor)
    [.allowedLateness(...)] <- optional: "lateness" (else zero)
    [.sideOutputLateData(...)] <- optional: "output tag" (else no side output for late data)
    .reduce/aggregate/apply() <- required: "function"
    [.getSideOutput(...)] <- optional: "output tag
     */

    //窗口聚合算子，整体上分为两类
    // 增量聚合算子，如 min、max、minBy、maxBy、sum、reduce、aggregate
    // 全量聚合算子，如 apply、process
    //两类聚合算子的底层区别
    // 增量聚合：一次取一条数据，用聚合函数对中间累加器更新；窗口触发时，取累加器输出结果；
    // 全量聚合：数据“攒”在状态容器中，窗口触发时，把整个窗口的数据交给聚合函数；


    //数据延迟处理
    //延迟处理的方案
    // 小延迟（乱序），用 watermark 容错 （减慢时间的推进，让本已迟到的数据被认为没有迟到）
    // 中延迟（乱序），用 allowedLateness （允许一定限度内的迟到，并对迟到数据重新触发窗口计算）
    // 大延迟（乱序），用 sideOutputLateData （将超出 allowedLateness 的迟到数据输出到一个侧流中）

    //具体案例： 没有分组的全局窗口，所有数据汇总到一个分区中
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dataStream = env.socketTextStream("master", 7777).map(s => {
      val arr = s.trim.split(",")
      (arr(0), arr(1).toLong)
    })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(t: (String, Long), l: Long): Long = t._2
          })

      )

    //统计每10秒钟数据条数
    val res = dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new AllWindowFunction[(String, Long), String, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
          val size = input.size
          out.collect(s"每10秒的数据总条数：${size}")
        }
      })
    res.print("res")
    // 乱序数据测试 => KeyedWindowsExample$
    env.execute()
  }
}
