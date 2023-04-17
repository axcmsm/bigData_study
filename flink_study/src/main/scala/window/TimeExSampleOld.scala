package window

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
@Deprecated
object TimeExSampleOld {
  def main(args: Array[String]): Unit = {
     //时间语义的设置
     val env = StreamExecutionEnvironment.getExecutionEnvironment
    //老版本的API  默认为 ProcessingTime 处理时间
    import org.apache.flink.streaming.api.TimeCharacteristic
    //设置 EventTime 作为时间标准
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置 IngestionTime 作为时间标准
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    //设置 ProcessingTime 作为时间标准
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    // watermark 的生成周期（默认值即为 200ms）
    env.getConfig.setAutoWatermarkInterval(200)

    //watermark的提取
    val data = env.socketTextStream("master", 7777)
    data.map(s=>{
      val arr = s.split(",")
      (arr(0),arr(1).toLong)
    })
      //.assignAscendingTimestamps(_._2)//没有乱序数据情况
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {//延迟5秒
        override def extractTimestamp(t: (String, Long)): Long = t._2
      })

  }
}
