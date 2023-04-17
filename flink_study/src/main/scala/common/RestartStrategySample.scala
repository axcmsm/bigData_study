package common

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration

import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 重启策略
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object RestartStrategySample {
  def main(args: Array[String]): Unit = {
    //Fixed Delay Restart Strategy：固定延迟重启策略
    //Exponential Delay Restart Strategy ：本策略：故障越频繁，两次重启间的惩罚间隔就越长
    //Failure Rate Restart Strategy：失败率重启策略
    //No Restart Strategy：无重启策略；
    //Fallback Restart Strategy 回退策略：


    /* val env = StreamExecutionEnvironment.getExecutionEnvironment
     env.setRestartStrategy(
       RestartStrategies.fixedDelayRestart(3,Time.of(10,TimeUnit.SECONDS))
      )*/

    //cluster 级失败重启恢复状态
    /** *
     * save points 相关配置（flink-conf.yaml）
     * state.savepoints.dir: hdfs:///flink/savepoints
     *
     */

    // 集群运行时，手动触发 savepoin
    //# 触发一次 savepoint
    //$ bin/flink savepoint :jobId [:targetDirectory]

    //# 为 yarn 模式集群触发 savepoint
    //$ bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId

    //# 停止一个 job 集群并触发 savepoint
    //$ bin/flink stop --savepointPath [:targetDirectory] :jobId

    //# 从一个指定 savepoint 恢复启动 job 集群
    //$ bin/flink run -s :savepointPath [:runArgs]

    //# 删除一个 savepoint
    //$ bin/flink savepoint -d :savepointPath

    //集群运行时，指定目录恢复 savepoint
    //# 从一个指定 savepoint 恢复启动 job 集群
    //$ bin/flink run -s :savepointPath [:runArgs]

    //# 删除一个 savepoint
    //$ bin/flink savepoint -d :savepointPath

    //集群运行时，删除指定 savepoint
    //# 删除一个 savepoint
    //$ bin/flink savepoint -d :savepointPath

  }
}
