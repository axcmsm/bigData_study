package window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
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
object KeyedWindowsExSample {
  def main(args: Array[String]): Unit = {
    /**
     * KeyedWindows 模板
     * stream
     * .keyBy(...) <- keyed versus non-keyed windows
     * .window(...) <- required: "assigner"
     * [.trigger(...)] <- optional: "trigger" (else default trigger)
     * [.evictor(...)] <- optional: "evictor" (else no evictor)
     * [.allowedLateness(...)] <- optional: "lateness" (else zero)
     * [.sideOutputLateData(...)] <- optional: "output tag" (else no side output for late data)
     * .reduce/aggregate/apply() <- required: "function"
     * [.getSideOutput(...)] <- optional: "output tag"
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

    //具体案例： 测试乱序数据
    // 数据格式：id,time
    /**
     * -------
     * 1,1681709311000
     * 1,1681709321000
     * 2,1681709315000
     * 2,1681709322000
     * ----1. 10 秒 促发窗口 乱序数据促发  => 窗口：1681709320000
     * 1,1681709313000    -> 会重复触发计算
     * 1,1681709311000    -> 会重复触发计算
     * 2,1681709319000   -> 会重复触发计算
     * 2,1681709320000   -> 不会重复触发计算
     * 3,1681709320000   -> 不会没有触发计算
     * ------------------  第一个窗口
     * result> ID:1,Count:1,EndTime:1681709320000
     * result> ID:2,Count:1,EndTime:1681709320000
     * result> ID:1,Count:2,EndTime:1681709320000
     * result> ID:1,Count:3,EndTime:1681709320000
     * result> ID:2,Count:2,EndTime:1681709320000
     * ------------------
     * ----2.  14 秒
     * 1,1681709324000   -> 假设数据 在14秒时
     * 1,1681709314000   -> 迟到数据  仍可以促发计算
     * 1,1681709325000   -> 假设数据 在15秒时，窗口结束，迟到数据输出测输出流中
     * 2,1681709314000   -> 迟到数据 最终输出到侧输出流，往后迟到的数据都输出到侧迟到流中
     * 1,1681709318000   -> 迟到数据  往后迟到的数据都输出到侧迟到流中
     * 1,1681709330000   -> 没有触发第二个窗口计算
     * 1,1681709332000   -> 12秒触发
     */

      //通过以上测试案例，可以发现：
      //1. 窗口的触发时机输出时间间隔是：  窗口长度+Watermark
      //2. 如果时间小于 allowedLateness 允许时间，迟到数据进入窗口后，将会再次触发窗口
      //3. 如果时间大于 allowedLateness 允许时间，窗口结束，迟到的数据会输出到测输出流中

    //使用time作为事件时间戳，允许数据迟到2秒，窗口的最大延迟时间为3秒，迟到的数据使用测输出流收集
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

    //Late
    val lateDataTag = new OutputTag[(String, Long)]("late_data")
    //求出10秒内，每个id的次数
    val keyByStream = dataStream.keyBy(_._1)
    val result = keyByStream
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(3)) // 20秒
      .sideOutputLateData(lateDataTag)
      .aggregate(new AggregateFunction[(String, Long), Long, Long]() {
        //聚合算子
        override def createAccumulator(): Long = 0L

        override def add(in: (String, Long), acc: Long): Long = acc + 1

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
      }, new WindowFunction[Long, String, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
          //格式化输出结果
          val cnt = input.head
          out.collect(s"ID:${key},Count:${cnt},EndTime:${window.getEnd}")
        }
      })
    result.print("result")

    //获取迟到数据
    result.getSideOutput(lateDataTag).print("lateDat")

    env.execute()

  }
}
