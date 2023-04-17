package window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object TimeExSampleNew {
  def main(args: Array[String]): Unit = {
    //时间语义的设置
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //新版本的API  Flink 1.12 及以后，flink 以 event time 作为默认的时间语义
    //在需要指定时间语义的相关操作（如时间窗口）时，可以通过显式的 api 来使用特定的时间语义；
    val data = env.socketTextStream("master", 7777).map(s=>{
      val arr = s.split(",")
      (arr(0),arr(1).toLong)
    })

    // EventTime 作为时间语义
    data.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
    data.windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))

    // Processing 作为时间语义
    data.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    data.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))


    //新版 api 中禁用事件时间语义
    // 如果设置为 0，则禁用了 watermark 的生成；从而失去了 event time 语义
    // env.getConfig.setAutoWatermarkInterval(0L)
    // watermark 的生成周期（默认值即为 200ms）
    env.getConfig.setAutoWatermarkInterval(200)
    //提示：如果需要使用已过期的 ingestion time，可以通过设置恰当的 watermark 来实现；

    //内置 watermark 生成策略
    //此前有两种生成时机策略：
    // AssignerWithPeriodicWatermarks 周期性生成 watermark
    // AssignerWithPunctuatedWatermarks[已过期] 按指定标记性事件生成 watermark
   // 在 flink1.12 后，watermark 默认是按固定频率周期性地产生；

    data
      //.assignAscendingTimestamps(_._2) //没有乱序数据情况
      //WatermarkStrategy.forMonotonousTimestamps(); 紧跟最大事件时间的 watermark 生成策略（完全不容忍乱序）
      //允许乱序的 watermark 生成策略（最大事件时间-容错时间）
      //WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)); // 根据实际数据的最大乱序情况来设置
      //自定义 watermark 生成策略
      //WatermarkStrategy.forGenerator(new WatermarkGenerator(){ ... } );
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness[(String,Long)](Duration.ZERO)
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(t: (String, Long), l: Long): Long = t._2
          })
          // 防止上游某些分区的水位线不推进导致下游的窗口一直不触发（这个分区很久都没数据）
          .withIdleness(Duration.ofMillis(2000))
      )

  }
}
