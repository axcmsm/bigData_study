package source

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 自定义读取Mysql数据源
 *
 * @author 须贺
 * @version 2023/4/15 
 */
object ReadMysqlData {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource)
    data.print()
    env.execute()
  }
}

/**
 * 自定义 source
    可以实现 SourceFunction 或者 RichSourceFunction , 这两者都是非并行的 source 算子
    也可实现 ParallelSourceFunction 或者 RichParallelSourceFunction , 这两者都是可并行的source 算子
  -- 带 Rich 的，都拥有 open() ,close() ,getRuntimeContext() 方法
  -- 带 Parallel 的，都可多实例并行执行
 */
class MyParallelSource() extends RichParallelSourceFunction[String] {
  val jdbcUrl = "jdbc:mysql://bigdata1:3306/ds_db01?useSSL=false&characterEncoding=utf8"
  val username = "root"
  val password = "123456"
  var connection: Connection = _
  var ps: PreparedStatement = _
  var rs: ResultSet = _

   var flag = true //定义一个 flag 标标志


  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    connection = DriverManager.getConnection(jdbcUrl, username, password)
    ps = connection.prepareStatement("SELECT * FROM brand_info")
  }

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while (flag){
      rs = ps.executeQuery()
      while (rs.next()){
        //品牌信息表：品牌ID，品牌名称，联系电话，'品牌状态,0禁用,1启用'
        val brand_id = rs.getString("brand_id") //品牌ID
        val brand_name = rs.getString("brand_name")//品牌名称
        val telephone = rs.getString("telephone") //联系电话
        val brand_status = rs.getString("brand_status")//'品牌状态,0禁用,1启用'
        sourceContext.collect(s"${brand_id},${brand_name},${telephone},${brand_status}")
        Thread.sleep(1000) //为避免太快，睡眠 1 秒
      }
    }
  }

  override def cancel(): Unit = {
    flag = false
    if (rs != null) rs.close
    if (ps != null) ps.close
    if (connection != null) connection.close
  }
}
