package sink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import source.MyParallelSource

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 写入Redis
 * @author 须贺
 * @version 2023/4/16 
 */
object RedisSinkSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("file:///E:/ckpt")
    val data = env.addSource(new MyParallelSource).setParallelism(1)
    //统计个数
    val res = data.map(_ => ("count", 1)).keyBy(_._1).sum(1)

    //写入Redis
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("bigdata1")
      .setPort(6379)
      .setDatabase(0)
      //.setPassword()
      .build()
    res.map(_._2.toString).addSink(new RedisSink[String](config,new MyRedisSet("cnt_set")))
    res.map(iter=>(iter._1,iter._2.toString)).addSink(new RedisSink[(String, String)](config,new MyRedisHset("cnt_hset")))

    // GET cnt_set
    // HGET cnt_hset
    env.execute()
  }
}

/**
 * key value 类型 => SET
 * @param key
 */
class MyRedisSet(key:String) extends RedisMapper[String]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }
  override def getKeyFromData(t: String): String =key
  override def getValueFromData(t: String): String = t
}

/**
 * key (key,value） 类型 => HSET
 * @param key
 */
class MyRedisHset(key:String) extends RedisMapper[(String,String)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,key)
  }
  override def getKeyFromData(t: (String, String)): String = t._1
  override def getValueFromData(t: (String, String)): String = t._2
}