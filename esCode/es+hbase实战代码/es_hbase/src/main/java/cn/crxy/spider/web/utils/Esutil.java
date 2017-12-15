package cn.crxy.spider.web.utils;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;

import cn.crxy.spider.web.domain.Article;

public class Esutil {
	// 设置client.transport.sniff为true来使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中，
		static Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elasticsearch")
				.put("client.transport.sniff", true).build();
		// 创建私有对象
		private static TransportClient client;
		static {
			try {
				Class<?> clazz = Class.forName(TransportClient.class.getName());
				Constructor<?> constructor = clazz
						.getDeclaredConstructor(Settings.class);
				//启用或禁用安全检查，值为true则表示反射的对象在使用时应该取消 Java语言访问检查
				constructor.setAccessible(true);
				client = (TransportClient) constructor.newInstance(settings);
				client.addTransportAddress(new InetSocketTransportAddress(
						"192.168.1.170", 9300));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		/**
		 * 获取客户端
		 * @return
		 */
		public static synchronized TransportClient getTransportClient() {
			return client;
		}
	
	
	
	
	public static String addIndex(String index,String type,Article article){
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		hashMap.put("id", article.getId());
		hashMap.put("title", article.getTitle());
		hashMap.put("describe", article.getDescribe());
		hashMap.put("author", article.getAuthor());
		
		IndexResponse response = getTransportClient().prepareIndex(index, type).setSource(hashMap).execute().actionGet();
		return response.getId();
	}
	
	
	public static Map<String, Object> search(String key,String index,String type,int start,int row){
		SearchRequestBuilder builder = getTransportClient().prepareSearch(index);
		builder.setTypes(type);
		builder.setFrom(start);
		builder.setSize(row);
		//设置高亮字段名称
		builder.addHighlightedField("title");
		builder.addHighlightedField("describe");
		//设置高亮前缀
		builder.setHighlighterPreTags("<font color='red' >");
		//设置高亮后缀
		builder.setHighlighterPostTags("</font>");
		builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		if(StringUtils.isNotBlank(key)){
			builder.setQuery(QueryBuilders.multiMatchQuery(key, "title","describe"));
		}
		builder.setExplain(true);
		SearchResponse searchResponse = builder.get();
		
		SearchHits hits = searchResponse.getHits();
		long total = hits.getTotalHits();
		Map<String, Object> map = new HashMap<String,Object>();
		SearchHit[] hits2 = hits.getHits();
		map.put("count", total);
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		for (SearchHit searchHit : hits2) {
			Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
			HighlightField highlightField = highlightFields.get("title");
			Map<String, Object> source = searchHit.getSource();
			if(highlightField!=null){
				Text[] fragments = highlightField.fragments();
				String name = "";
				for (Text text : fragments) {
					name+=text;
				}
				source.put("title", name);
			}
			HighlightField highlightField2 = highlightFields.get("describe");
			if(highlightField2!=null){
				Text[] fragments = highlightField2.fragments();
				String describe = "";
				for (Text text : fragments) {
					describe+=text;
				}
				source.put("describe", describe);
			}
			list.add(source);
		}
		map.put("dataList", list);
		return map;
	}
	
}
