package window

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows


/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object WindowExSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //窗口计算案例
    //滚动窗口，滑动窗口，会话窗口
    val dataStream = env.socketTextStream("master", 7777)

    /**
     * NonKeyed 窗口，全局窗口
     *//**
     * NonKeyed 窗口，全局窗口
     */
    // 处理时间语义，滚动窗口
    dataStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    // 处理时间语义，滑动窗口
    dataStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    // 事件时间语义，滚动窗口
    dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
    // 事件时间语义，滑动窗口
    dataStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    // 计数滚动窗口
    dataStream.countWindowAll(100)
    // 计数滑动窗口
    dataStream.countWindowAll(100, 20)


    /**
     * Keyed 窗口
     *//**
     * Keyed 窗口
     */
    val keyedStream = dataStream.keyBy(_=>true)
    // 处理时间语义，滚动窗口
    keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    // 处理时间语义，滑动窗口
    keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    // 事件时间语义，滚动窗口
    keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    // 事件时间语义，滑动窗口
    keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    // 计数滚动窗口
    keyedStream.countWindow(1000)
    // 计数滑动窗口
    keyedStream.countWindow(1000, 100)
  }
}
