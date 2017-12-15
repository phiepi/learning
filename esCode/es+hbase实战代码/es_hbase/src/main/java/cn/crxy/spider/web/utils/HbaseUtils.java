package cn.crxy.spider.web.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import cn.crxy.spider.web.domain.Article;

/**
 * 建表语句
 * create 'article','info'
 * @author Administrator
 *
 */
public class HbaseUtils {
	
	/**
	 * HBASE 表名称
	 */
	public  final String TABLE_NAME = "article";
	/**
	 * 列簇1 文章信息
	 */
	public  final String COLUMNFAMILY_1 = "info";
	/**
	 * 列簇1中的列
	 */
	public  final String COLUMNFAMILY_1_TITLE = "title";
	public  final String COLUMNFAMILY_1_AUTHOR = "author";
	public  final String COLUMNFAMILY_1_CONTENT = "content";
	public  final String COLUMNFAMILY_1_DESCRIBE = "describe";
	
	
	
	HBaseAdmin admin=null;
	Configuration conf=null;
	/**
	 * 构造函数加载配置
	 */
	public HbaseUtils(){
		conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "119.90.51.196:2181");
		/**
		 * 注意：如果使用这个地址无法访问的话请尝试使用这个
		 * hdfs://119.90.51.196:8020/hbase
		 * 因为hadoop这边做了HA，现在主为197，备为196
		 */
		conf.set("hbase.rootdir", "hdfs://119.90.51.197:8020/hbase");
		try {
			admin = new HBaseAdmin(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws Exception {
		HbaseUtils hbase = new HbaseUtils();
		//创建一张表
//		hbase.createTable("stu10","cf");
//		//查询所有表名
		hbase.getALLTable();
//		//往表中添加一条记录
//		hbase.put(hbase.TABLE_NAME, "1", hbase.COLUMNFAMILY_1, hbase.COLUMNFAMILY_1_AUTHOR, "crxy");
//		hbase.addOneRecord("stu","key1","cf","age","24");
//		//查询一条记录
//		hbase.getKey("stu","key1");
//		//获取表的所有数据
//		hbase.getALLData("stu");
//		//删除一条记录
//		hbase.deleteOneRecord("stu","key1");
//		//删除表
//		hbase.deleteTable("stu");
		//scan过滤器的使用
//		hbase.getScanData("stu","cf","age");
		//rowFilter的使用
		//84138413_20130313145955
		//hbase.getRowFilter("waln_log","^*_201303131400\\d*$");
	}
	/**
	 * rowFilter的使用
	 * @param tableName
	 * @param reg
	 * @throws Exception
	 */
	public void getRowFilter(String tableName, String reg) throws Exception {
		HTable hTable = new HTable(conf, tableName);
		Scan scan = new Scan();
//		Filter
		RowFilter rowFilter = new RowFilter(CompareOp.NOT_EQUAL, new RegexStringComparator(reg));
		scan.setFilter(rowFilter);
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(new String(result.getRow()));
		}
	}
	
	public void getScanData(String tableName, String family, String qualifier) throws Exception {
	HTable hTable = new HTable(conf, tableName);
	Scan scan = new Scan();
	scan.addColumn(family.getBytes(), qualifier.getBytes());
	ResultScanner scanner = hTable.getScanner(scan);
	for (Result result : scanner) {
		if(result.raw().length==0){
			System.out.println(tableName+" 表数据为空！");
		}else{
			for (KeyValue kv: result.raw()){
				System.out.println(new String(kv.getKey())+"\t"+new String(kv.getValue()));
			}
		}
	}
	}
	private void deleteTable(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName+"表删除成功！");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(tableName+"表删除失败！");
		}
		
	}
	/**
	 * 删除一条记录
	 * @param tableName
	 * @param rowKey
	 */
	public void deleteOneRecord(String tableName, String rowKey) {
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Delete delete = new Delete(rowKey.getBytes());
		try {
			table.delete(delete);
			System.out.println(rowKey+"记录删除成功！");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(rowKey+"记录删除失败！");
		}
	}
	/**
	 * 获取表的所有数据
	 * @param tableName
	 */
	public void getALLData(String tableName) {
		try {
			HTable hTable = new HTable(conf, tableName);
			Scan scan = new Scan();
			ResultScanner scanner = hTable.getScanner(scan);
			for (Result result : scanner) {
				if(result.raw().length==0){
					System.out.println(tableName+" 表数据为空！");
				}else{
					for (KeyValue kv: result.raw()){
						System.out.println(new String(kv.getKey())+"\t"+new String(kv.getValue()));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	// 读取一条记录
		@SuppressWarnings({ "deprecation", "resource" })
		public  Article get(String tableName, String row) {
			HTablePool hTablePool = new HTablePool(conf, 1000);
			HTableInterface table = hTablePool.getTable(tableName);
			Get get = new Get(row.getBytes());
			Article article = null;
			try {
				
				Result result = table.get(get);
				KeyValue[] raw = result.raw();
				if (raw.length == 4) {
					article = new Article();
					article.setId(Integer.parseInt(row));
					article.setTitle(new String(raw[3].getValue()));
					article.setAuthor(new String(raw[0].getValue()));
					article.setContent(new String(raw[1].getValue()));
					article.setDescribe(new String(raw[2].getValue()));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return article;
		}
		
		
		// 添加一条记录
		public  void put(String tableName, String row, String columnFamily,
				String column, String data) throws IOException {
			HTablePool hTablePool = new HTablePool(conf, 1000);
			HTableInterface table = hTablePool.getTable(tableName);
			Put p1 = new Put(Bytes.toBytes(row));
			p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
					Bytes.toBytes(data));
			table.put(p1);
			System.out.println("put'" + row + "'," + columnFamily + ":" + column
					+ "','" + data + "'");
		}
	
	
	/**
	 * 查询所有表名
	 * @return
	 * @throws Exception
	 */
	public List<String> getALLTable() throws Exception {
		ArrayList<String> tables = new ArrayList<String>();
		if(admin!=null){
			HTableDescriptor[] listTables = admin.listTables();
			if (listTables.length>0) {
				for (HTableDescriptor tableDesc : listTables) {
					tables.add(tableDesc.getNameAsString());
					System.out.println(tableDesc.getNameAsString());
				}
			}
		}
		return tables;
	}
	/**
	 * 创建一张表
	 * @param tableName
	 * @param column
	 * @throws Exception
	 */
	public void createTable(String tableName, String column) throws Exception {
		if(admin.tableExists(tableName)){
			System.out.println(tableName+"表已经存在！");
		}else{
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(column.getBytes()));
			admin.createTable(tableDesc);
			System.out.println(tableName+"表创建成功！");
		}
	}
}
