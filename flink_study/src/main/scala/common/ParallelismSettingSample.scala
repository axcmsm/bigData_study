package common

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.MyParallelSource

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object ParallelismSettingSample {
  def main(args: Array[String]): Unit = {
    //并行度相关API
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    //1. 全局并行度
    //全局并行度是指整个 Flink Job 在运行时所使用的并行度，它会影响到 Job 的吞吐量和可伸缩性。可以通过调整全局并行度来优化作业的性能。
    //env.setParallelism(4)
   //在上面的示例中，我们将 Job 的全局并行度设置为 4，这意味着整个 Job 运行时会使用 4 个并行计算资源。

    //2. 算子并行度
    //算子并行度是指单个算子在执行时所使用的并行计算资源的数量，通常是由全局并行度和任务拓扑结构共同决定的。在 Flink 中，可以对每个算子独立设置其并行度.
    val data = env.addSource(new MyParallelSource).setParallelism(2)
    //需要注意的是，在设置算子并行度时，需要根据当前任务的计算资源和任务拓扑结构等因素综合考虑。如果并行度设置过高，可能会导致计算资源浪费；而如果设置过低，可能会导致任务运行缓慢



    //对算子链的操作
    // slotSharingGroup 设置算子的槽位共享组
    // disableChaining 对算子禁用前后链合并
    // startNewChain 对算子开启新链（即禁用算子前链合并）


    //分区算子：用于指定上游 task 的各并行 subtask 与下游 task 的 subtask 之间如何传输数据；
    //Flink 中，对于上下游 subTask 之间的数据传输控制，由 ChannelSelector 策略来控制，而且 Flink 内针
    //对各种场景，开发了众多 ChannelSelector 的具体实现
    data.global //全部发往一个task
    data.broadcast() //广播
    data.forward // 上下游并发度一样时，一对一发送
    data.shuffle //随机均匀分配
    data.rebalance //轮流分配
    data.rescale //本地轮流分配
    //data.partitionCustom() //自定义单播
    //data.keyBy() //根据key的hashcode进行hash分发
    //默认情况下，flink 会优先使用 REBALANCE 分发策略  轮询


  }
}
