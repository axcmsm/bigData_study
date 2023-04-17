package state

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 状态后端设置
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object BackendStateSample {
  def main(args: Array[String]): Unit = {
    //可用的状态后端类型（flink-1.13 版以后）
    // HashMapStateBackend
    // EmbeddedRocksDBStateBackend
    //新版本中，Fsstatebackend 和 MemoryStatebackend 整合成了 HashMapStateBackend
    //而且 HashMapStateBackend 和 EmBeddedRocksDBStateBackend 所生成的快照文件也统一了格式，因而
    //在 job 重新部署或者版本升级时，可以任意替换 statebackend

    //rocksdb-backend
    //<dependency>
    //<groupId>org.apache.flink</groupId>
    //<artifactId>flink-statebackend-rocksdb_2.12</artifactId>
    //<version>${flink.version}</version>
    //</dependency>
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStateBackend(new )
    //配置文件中配置
    //# The backend that will be used to store operator state checkpoints
    //state.backend: hashmap
    //# Directory for storing checkpoints
    //state.checkpoints.dir: hdfs://doitedu:8020/flink/checkpoints


    //HashMapStateBackend
    // 状态数据是以 java 对象形式存储在 heap 内存中；
    // 内存空间不够时，也会溢出一部分数据到本地磁盘文件；
    // 可以支撑大规模的状态数据；（只不过在状态数据规模超出内存空间时，读写效率就会明显降低）
    //对于 KeyedState 来说 ，HashMapStateBackend 在内存中是使用 CopyOnWriteStateMap 结构来存储用户的状态数据；
    //注意，此数据结构类，名为 Map，实非 Map，它其实是一个单向链表的数据结构
    //对于 OperatorState 来说：可以清楚看出，它底层直接用一个 Map 集合来存储用户的状态数据：状态名称 --> 状态 List


    //EmBeddedRocksDbStateBackend 状态后端
    //状态数据是交给 rocksdb 来管理；
    // Rocksdb 中的数据是以序列化的 kv 字节进行存储；
    // Rockdb 中的数据，有内存缓存的部分，也有磁盘文件的部分；
    // Rockdb 的磁盘文件数据读写速度相对还是比较快的，所以在支持超大规模状态数据时，数据的
    //读写效率不会有太大的降低


  }
}
