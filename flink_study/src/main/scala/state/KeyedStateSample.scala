package state

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 键控状态
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object KeyedStateSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(2000)
    env.getCheckpointConfig.setCheckpointStorage("file:///E:/ckpt")


    // 读取自定义数据源
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

    /**
     * ValueState<T>: 保存一个可以更新和检索的值（如上所述，每个值都对应到当前的输入数据的 key，因此算子接收到的每个 key 都可能对应一个值）。 这个值可以通过 update(T) 进行更新，通过 T value() 进行检索。
     * ListState<T>: 保存一个元素的列表。可以往这个列表中追加数据，并在当前的列表上进行检索。可以通过 add(T) 或者 addAll(List<T>) 进行添加元素，通过 Iterable<T> get() 获得整个列表。还可以通过 update(List<T>) 覆盖当前的列表。
     * ReducingState<T>: 保存一个单值，表示添加到状态的所有值的聚合。接口与 ListState 类似，但使用 add(T) 增加元素，会使用提供的 ReduceFunction 进行聚合。
     * AggregatingState<IN, OUT>: 保留一个单值，表示添加到状态的所有值的聚合。和 ReducingState 相反的是, 聚合类型可能与 添加到状态的元素的类型不同。 接口与 ListState 类似，但使用 add(IN) 添加的元素会用指定的 AggregateFunction 进行聚合。
     * MapState<UK, UV>: 维护了一个映射列表。 你可以添加键值对到状态中，也可以获得反映当前所有映射的迭代器。使用 put(UK，UV) 或者 putAll(Map<UK，UV>) 添加映射。 使用 get(UK) 检索特定 key。 使用 entries()，keys() 和 values() 分别检索映射、键和值的可迭代视图。你还可以通过 isEmpty() 来判断是否包含任何键值对。
     * 所有类型的状态还有一个clear() 方法，清除当前 key 下的状态数据，也就是当前输入元素的 key。
     */


    //使用状态编程+定时器 每个商品在10秒内，金额连续上升商品，进行输出 (10秒输出一次数据，不会触发触发，例：20秒内只输出俩次）
    // 输出格式：商品：花纹，最高金额：200 仍在连续上升
    val res = streamBrandInfo.keyBy(_.brand_id).process(new KeyedProcessFunction[String, Data_Event, String] {
      lazy val valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("amount_state", classOf[Double]))
      lazy val ts_timer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ts_timer", classOf[Long]))
      override def processElement(i: Data_Event, context: KeyedProcessFunction[String, Data_Event, String]#Context, collector: Collector[String]): Unit = {
        val ts = ts_timer.value()
        val amount = valueState.value()
        if(i.amount>=amount){
          valueState.update(i.amount)
          if(ts==0){
            val new_ts = i.time + (10 * 1000)
            ts_timer.update(new_ts)
            context.timerService().registerEventTimeTimer(new_ts)
          }
        }else{
          valueState.clear()
          ts_timer.clear()
          context.timerService().deleteEventTimeTimer(ts)
        }
      }
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Data_Event, String]#OnTimerContext, out: Collector[String]): Unit = {
        val amount = valueState.value()
        out.collect(s"商品：${ctx.getCurrentKey}，最高金额：${amount} 仍在连续上升")
        valueState.clear()
        ts_timer.clear()
      }
    })
    res.print("res")

    env.execute()
  }
}

case class Data_Event(
                       brand_id: String,
                       brand_name: String,
                       amount: Double,
                       time: Long
                     )
