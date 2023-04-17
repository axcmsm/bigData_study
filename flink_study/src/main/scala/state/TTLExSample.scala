package state

import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility
import org.apache.flink.api.common.state.{ListStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 状态TTL管理
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object TTLExSample {
  def main(args: Array[String]): Unit = {
    //状态TTL管理
    // flink 可以对状态数据进行存活时长管理；
    // 淘汰的机制主要是基于存活时间；
    // 存活时长的计时器可以在数据被读、写时重置；
    // Ttl 存活管理粒度是到元素级的（如 liststate 中的每个元素，mapstate 中的每个 entry）
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.enableCheckpointing(2000)
//    env.getCheckpointConfig.setCheckpointStorage("file:///E:/ckpt")
    /**
     * 1,花纹,200.0,1681709311000
     * 1,花纹,400.0,1681709312000
     * 1,花纹,600.0,1681709314000
     * 1,花纹,700.0,1681709320000
     * 1,花纹,900.0,1681709323000
     */
    val streamBrandInfo = env.socketTextStream("master", 7777).map(iter => {
      val arr = iter.trim.split(",")
      Data_Event(arr(0), arr(1), arr(2).toDouble, arr(3).toLong)
    })
      .assignAscendingTimestamps(_.time)
    streamBrandInfo.print()
    val res = streamBrandInfo.keyBy(_=>true).process(new KeyedProcessFunction[Boolean,Data_Event, String] {
      var cnt_State: ValueState[Long] = _
      override def open(parameters: Configuration): Unit = {
        //设置ttl
        val valueDescriptor = new ValueStateDescriptor[Long]("cnt_state", classOf[Long])
        val ttlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
          .useProcessingTime() // 默认是用 eventTime 语义，如果需要用 processingTime 语义，则需要显式指定
          //.updateTtlOnReadAndWrite() // ttl 重置刷新策略：数据只要被读取或者被写更新，则将它的 ttl 计时重置
          //.updateTtlOnCreateAndWrite() // ttl 重置刷新策略：数据被创建及被写入更新，就将它的 ttl 计时重置
          // 状态数据的可见性：如果状态中存在还未清理掉的但是已经超出 ttl 的数据，是否让用户程序可见
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          .build()
        valueDescriptor.enableTimeToLive(ttlConfig)
        cnt_State = getRuntimeContext.getState(valueDescriptor)
      }

      override def processElement(i: Data_Event, context: KeyedProcessFunction[Boolean, Data_Event, String]#Context, collector: Collector[String]): Unit = {
        val new_cnt = cnt_State.value() + 1
        cnt_State.update(new_cnt)
        collector.collect(new_cnt.toString)
      }
    })

    res.print()
    env.execute()
  }
}
