import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import sink.Brand_Info

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/16 
 */
object TestWriteToHbase {
  def main(args: Array[String]): Unit = {
    //  res.addSink(new MyHbaseSink("brand_info","cf:brand_id,cf:brand_name,cf:telephone,cf:brand_status"))
    val configuration= HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "bigdata1:2181,bigdata2:2181,bigdata3:2181")
    val connection = ConnectionFactory.createConnection(configuration)
    val admin = connection.getAdmin
    val tableName = TableName.valueOf("brand_info")
    if(!admin.tableExists(tableName)){
      admin.createTable(
        new HTableDescriptor(tableName)
          .addFamily(new HColumnDescriptor("cf"))
      )
    }
    val table = connection.getTable(tableName)
    val brandInfo =Brand_Info("001", "Apple", "888-888-8888", "Active")
    val put = new Put(Bytes.toBytes(brandInfo.brand_id))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("brand_name"), Bytes.toBytes(brandInfo.brand_name))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("telephone"), Bytes.toBytes(brandInfo.telephone))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("brand_status"), Bytes.toBytes(brandInfo.brand_status))
    table.put(put)
    table.close()
    admin.close()
    connection.close()
  }
}
//给hbase建立外部表
/**
create external table brand_info(
brand_id String,
brand_name String,
telephone String,
brand_status String
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,cf:brand_name,cf:telephone,cf:brand_status")
tblproperties("hbase.table.name"="brand_info");
select *
from brand_info;
 */