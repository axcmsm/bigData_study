package table.tableConnector

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object HiveConnectorSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = StreamTableEnvironment.create(env)

    // TODO: hive connect
    val catalogTypename: String = "hive"
    val defaultDatabase: String = "default"
    val hiveConfDir: String = "D:/hiveconf/"
    import org.apache.flink.table.catalog.hive.HiveCatalog
    // 创建和注册 hive catalog

    val hive = new HiveCatalog(catalogTypename, defaultDatabase, hiveConfDir)
    tableEnvironment.registerCatalog("myhive", hive)
    // set the HiveCatalog as the current catalog of the session
    tableEnvironment.useCatalog("myhive")
    // 打印当前 env 中所有的 catalog
    tableEnvironment.executeSql("show catalogs").print()
    // 查询 hive 中的表
    tableEnvironment.executeSql("select * from t2").print()
  }
}
