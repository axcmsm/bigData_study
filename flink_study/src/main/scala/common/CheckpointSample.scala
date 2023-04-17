package common

import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.time.Duration

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 检查点相关api
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object CheckpointSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointStorage(new Path("hdfs://master:8020/flink-jobs-checkpoints/"))
    // 容许 checkpoint 失败的最大次数
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(10)
    // checkpoint 的算法模式（是否需要对齐）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // job 取消时是否保留 checkpoint 数据
    //env.getCheckpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    // 设置 checkpoint 对齐的超时时间
    env.getCheckpointConfig.setAlignedCheckpointTimeout(Duration.ofMillis(2000))
    // 两次 checkpoint 的最小间隔时间，为了防止两次 checkpoint 的间隔时间太短
    env.getCheckpointConfig.setCheckpointInterval(2000)
    // 最大并行的 checkpoint 数
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(3)
    // 要用状态，最好还要指定状态后端(默认是 HashMapStateBackend)
    //env.setStateBackend(new EmbeddedRocksDBStateBackend());
    env.setStateBackend(new HashMapStateBackend());
  }
}
