package cn.crxy.elasticsearch_08;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestEs {
	TransportClient client;
	@Before
	public void test1() throws Exception {
		//获取一个Transport客户端对象
		client = new TransportClient();
		//指定连接的es节点ip和端口，端口在这默认使用9300
		client.addTransportAddress(new InetSocketTransportAddress("192.168.1.170", 9300));
		client.addTransportAddress(new InetSocketTransportAddress("192.168.1.171", 9300));
		//获取transport客户端已经连接上的节点信息
		/*ImmutableList<DiscoveryNode> connectedNodes = transportClient.connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println(discoveryNode.getHostAddress());
		}*/
	}
	
	@Test
	public void test2() throws Exception {
		//设置一些配置属性
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		//获取一个Transport客户端对象
		TransportClient transportClient = new TransportClient(settings);
		//指定连接的es节点ip和端口，端口在这默认使用9300
		transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.1.170", 9300));
		transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.1.171", 9300));
		//获取transport客户端已经连接上的节点信息
		ImmutableList<DiscoveryNode> connectedNodes = transportClient.connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println(discoveryNode.getHostAddress());
		}
	}
	
	/**
	 * 建议使用这种方式
	 * @throws Exception
	 */
	@Test
	public void test3() throws Exception {
		//设置一些配置属性（可以自动嗅探集群中的其它节点）
		Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).build();
		//获取一个Transport客户端对象
		TransportClient transportClient = new TransportClient(settings);
		//指定连接的es节点ip和端口，端口在这默认使用9300
		transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.1.170", 9300));
		//获取transport客户端已经连接上的节点信息
		ImmutableList<DiscoveryNode> connectedNodes = transportClient.connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println(discoveryNode.getHostAddress());
		}
		
	}
	String index = "crxy";
	String type = "emp";
	
	/**
	 * 创建索引--json
	 * @throws Exception
	 */
	@Test
	public void test4() throws Exception {
		String jsonstr = "{\"name\":\"ls\",\"age\":10}";
		IndexResponse response = client.prepareIndex(index, type, "2").setSource(jsonstr).execute().actionGet();
		System.out.println(response.getId());
		
	}
	
	/**
	 * 创建索引--map
	 * @throws Exception
	 */
	@Test
	public void test5() throws Exception {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		hashMap.put("name", "ww");
		hashMap.put("age", 18);
		IndexResponse response = client.prepareIndex(index, type, "3").setSource(hashMap).execute().actionGet();
		System.out.println(response.getId());
	}
	
	/**
	 * 创建索引--bean
	 * @throws Exception
	 */
	@Test
	public void test6() throws Exception {
		Person person = new Person();
		person.setName("aa");
		person.setAge(10);
		
		ObjectMapper objectMapper = new ObjectMapper();
		IndexResponse response = client.prepareIndex(index, type, "4").setSource(objectMapper.writeValueAsString(person)).execute().actionGet();
		System.out.println(response.getId());
	}
	
	/**
	 * 创建索引--es 工具类
	 * @throws Exception
	 */
	@Test
	public void test7() throws Exception {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
			.field("name", "ww").field("age",20)
			.endObject();
		IndexResponse response = client.prepareIndex(index, type, "4").setSource(builder).execute().actionGet();
		System.out.println(response.getId());
	}
	
	
	/**
	 * 查询-get
	 * @throws Exception
	 */
	@Test
	public void test8() throws Exception {
		GetResponse response = client.prepareGet(index, type, "1").execute().actionGet();
		System.out.println(response.getSourceAsString());
	}
	
	/**
	 * 更新 --1
	 * @throws Exception
	 */
	@Test
	public void test9() throws Exception {
		UpdateRequest request = new UpdateRequest(index, type, "1");
		request.doc(XContentFactory.jsonBuilder().startObject().field("age", 18).endObject());
		UpdateResponse response = client.update(request ).get();
		System.out.println(response.getVersion());
	}
	
	/**
	 * 更新--2
	 * @throws Exception
	 */
	@Test
	public void test10() throws Exception {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("name", "crxy").endObject();
		
		UpdateResponse response = client.prepareUpdate(index, type, "1").setDoc(builder).get();
		System.out.println(response.getVersion());
	}
	
	
	/**
	 * 更新或者插入
	 * @throws Exception
	 */
	@Test
	public void test11() throws Exception {
		
		UpdateRequest request = new UpdateRequest(index, type, "11");
		request.doc(XContentFactory.jsonBuilder().startObject().field("name", "cxry123").endObject());
		request.upsert(XContentFactory.jsonBuilder().startObject().field("name", "aaaaa").field("age", 23).endObject());
		UpdateResponse response = client.update(request ).get();
		System.out.println(response.getVersion());
	}
	
	
	/**
	 * 删除
	 * @throws Exception
	 */
	@Test
	public void test12() throws Exception {
		DeleteRequest request = new DeleteRequest();
		request.index(index);
		request.type(type);
		request.id("11");
		DeleteResponse response = client.delete(request ).get();
		System.out.println(response.getVersion());
		
	}
	/**
	 * 查询指定索引库中数据的总数
	 * 相当于数据库中的select count(*)
	 * @throws Exception
	 */
	@Test
	public void test13() throws Exception {
		long count = client.prepareCount(index).execute().get().getCount();
		System.out.println(count);
	}
	
	
	/**
	 * 批量操作bulk
	 * 非常适合做一些数据批量处理
	 * @throws Exception
	 */
	@Test
	public void test14() throws Exception {
		BulkRequestBuilder prepareBulk = client.prepareBulk();
		//创建索引
		IndexRequest indexrequest = new IndexRequest(index, type, "12");
		indexrequest.source(XContentFactory.jsonBuilder().startObject().field("name", "test12").field("age", 12).endObject());
		//删除索引
		DeleteRequest deleteRequest = new DeleteRequest(index, type, "3");
		
		prepareBulk.add(indexrequest);
		prepareBulk.add(deleteRequest);
		
		BulkResponse response = prepareBulk.execute().actionGet();
		if(response.hasFailures()){
			System.out.println("出现执行失败的语句");
			BulkItemResponse[] items = response.getItems();
			for (BulkItemResponse bulkItemResponse : items) {
				System.out.println(bulkItemResponse.getFailureMessage());
			}
		}else{
			System.out.println("全部执行成功");
		}
	}
	
	/**
	 * 查询-search
	 * @throws Exception
	 */
	@Test
	public void test15() throws Exception {
		SearchResponse response = client
				//指定索引库
				.prepareSearch(index)
				//指定类型
				.setTypes(type)
				//指定查询类型
				.setSearchType(SearchType.QUERY_THEN_FETCH)
				//指定要查询的关键字
				.setQuery(QueryBuilders.matchQuery("name", "中国人"))
				//相当于实现分页
				.setFrom(0)
				.setSize(10)
				//执行查询
				.execute().actionGet();
		//可以获取查询的所有内容
		SearchHits hits = response.getHits();
		//获取查询的数据总数
		long totalHits = hits.getTotalHits();
		System.out.println("总数："+totalHits);
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSourceAsString());
		}
		
	}
	/**
	 * 排序
	 * @throws Exception
	 */
	@Test
	public void test16() throws Exception {
		SearchResponse response = client
				//指定索引库
				.prepareSearch(index)
				//指定类型
				.setTypes(type)
				//指定查询类型
				.setSearchType(SearchType.QUERY_THEN_FETCH)
				//添加排序字段并指定排序类型
				.addSort("age", SortOrder.DESC)
				//相当于实现分页
				.setFrom(0)
				.setSize(10)
				//执行查询
				.execute().actionGet();
		//可以获取查询的所有内容
		SearchHits hits = response.getHits();
		//获取查询的数据总数
		long totalHits = hits.getTotalHits();
		System.out.println("总数："+totalHits);
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSourceAsString());
		}
		
	}
	
	
	/**
	 * 过滤
	 * lt：小于
	 * lte:小于等于
	 * gt:大于
	 * gte:大于等于
	 * @throws Exception
	 */
	@Test
	public void test17() throws Exception {
		SearchResponse response = client
				//指定索引库
				.prepareSearch(index)
				//指定类型
				.setTypes(type)
				//指定查询类型
				.setSearchType(SearchType.QUERY_THEN_FETCH)
				.setPostFilter(FilterBuilders.rangeFilter("age").gte(20).lte(25))
				//相当于实现分页
				.setFrom(0)
				.setSize(10)
				//执行查询
				.execute().actionGet();
		//可以获取查询的所有内容
		SearchHits hits = response.getHits();
		//获取查询的数据总数
		long totalHits = hits.getTotalHits();
		System.out.println("总数："+totalHits);
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSourceAsString());
		}
		
	}
	
	
	/**
	 * 高亮
	 * @throws Exception
	 */
	@Test
	public void test18() throws Exception {
		SearchResponse response = client
				//指定索引库
				.prepareSearch(index)
				//指定类型
				.setTypes(type)
				//指定查询类型
				.setSearchType(SearchType.QUERY_THEN_FETCH)
				//指定要查询的关键字
				.setQuery(QueryBuilders.matchQuery("name", "中国人"))
				.addHighlightedField("name")
				.setHighlighterPreTags("<font color='red'>")
				.setHighlighterPostTags("</font>")
				//相当于实现分页
				.setFrom(0)
				.setSize(10)
				//执行查询
				.execute().actionGet();
		//可以获取查询的所有内容
		SearchHits hits = response.getHits();
		//获取查询的数据总数
		long totalHits = hits.getTotalHits();
		System.out.println("总数："+totalHits);
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			//获取所有高亮内容
			Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
			//根据设置的高亮字段获取高亮内容
			HighlightField highlightField = highlightFields.get("name");
			Text[] fragments = highlightField.getFragments();
			for (Text text : fragments) {
				System.out.println(text);
			}
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	/**
	 * 根据查询条件进行删除数据
	 * @throws Exception
	 */
	@Test
	public void test19() throws Exception {
		DeleteByQueryResponse response = client.prepareDeleteByQuery(index)
		.setQuery(QueryBuilders.matchAllQuery())
		.execute().actionGet();
	}
	
	/**
	 * 根据姓名分组，统计相同姓名有多少条数据
	 * @throws Exception
	 */
	@Test
	public void test20() throws Exception {
		SearchResponse response = client.prepareSearch(index).setTypes(type)
			.addAggregation(AggregationBuilders.terms("group").field("name"))
			//默认返回分组之后的前十条数据，设置为0之后会返回所有的数据
			.setSize(0)
			.execute().actionGet();
		Terms terms = response.getAggregations().get("group");
		List<Bucket> buckets = terms.getBuckets();
		for (Bucket bucket : buckets) {
			System.out.println(bucket.getKey()+"------"+bucket.getDocCount());
		}
	}
	
	
	/**
	 * (业务场景不合理)
	 * 根据姓名分组，统计相同姓名的年龄的和
	 * @throws Exception
	 */
	@Test
	public void test21() throws Exception {
		SearchResponse response = client.prepareSearch(index).setTypes(type)
			.addAggregation(AggregationBuilders.terms("group").field("name")
					.subAggregation(AggregationBuilders.sum("sum").field("age")))
			//默认返回分组之后的前十条数据，设置为0之后会返回所有的数据
			.setSize(0)
			.execute().actionGet();
		Terms terms = response.getAggregations().get("group");
		List<Bucket> buckets = terms.getBuckets();
		for (Bucket bucket : buckets) {
			Sum sum  = bucket.getAggregations().get("sum");
			System.out.println(bucket.getKey()+"------"+sum.getValue());
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
