package com.koudai.estest;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by liupinghe.
 */
public class ESTest {

    private TransportClient client;


    @Before
    public void before() throws Exception {

        ClientFactory clientFactory = new ClientFactory();
        clientFactory.setEsClusterName("my-application");
        clientFactory.setEsServerIps("10.8.96.11");
        clientFactory.setEsServerPort(9300);
        client = clientFactory.getTransPortClient();

    }


    @Test
    public void testInfo() {
        List<DiscoveryNode> nodes = client.connectedNodes();
        for (DiscoveryNode node : nodes) {
            System.out.println(node.getHostAddress());
        }
    }

    @Test
    public void test1() throws Exception {
        XContentBuilder source = ClientFactory.createJson4();
        // 存json入索引中
//        IndexResponse response = client.prepareIndex("twitter", "tweet", "4").setSource(source).get();
        IndexResponse response = client.prepareIndex("twitter", "tweet").setSource(source).get();
//        // 结果获取
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        boolean created = response.isFragment();
        String result = response.getResult().name();
        System.out.println(index + " : " + type + ": " + id + ": " + version + ": " + created + ": " + result);
    }

    /**
     * get API 获取指定文档信息
     */
    @Test
    public void testGet() {
//        GetResponse response = client.prepareGet("twitter", "tweet", "1")
//                                .get();
        GetResponse response = client.prepareGet("twitter", "tweet", "1")
                .setOperationThreaded(false)    // 线程安全
                .get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 测试 delete api
     */
    @Test
    public void testDelete() {
        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1")
                .get();
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        System.out.println(index + " : " + type + ": " + id + ": " + version);
    }

    /**
     * 测试更新 update API
     * 使用 updateRequest 对象
     *
     * @throws Exception
     */
    @Test
    public void testUpdate() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("twitter1");
        updateRequest.type("tweet");
        updateRequest.id("1");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("gender", "male")
                .field("message", "hello")
                .endObject());
        UpdateResponse response = client.update(updateRequest).get();

        // 打印
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        System.out.println(index + " : " + type + ": " + id + ": " + version);
    }

    /**
     * 测试update api, 使用client
     *
     * @throws Exception
     */
    @Test
    public void testUpdate2() throws Exception {
        // 使用Script对象进行更新
//        UpdateResponse response = client.prepareUpdate("twitter", "tweet", "1")
//                .setScript(new Script("hits._source.gender = \"male\""))
//                .get();

        // 使用XContFactory.jsonBuilder() 进行更新
//        UpdateResponse response = client.prepareUpdate("twitter", "tweet", "1")
//                .setDoc(XContentFactory.jsonBuilder()
//                        .startObject()
//                            .field("gender", "malelelele")
//                        .endObject()).get();

        // 使用updateRequest对象及script
//        UpdateRequest updateRequest = new UpdateRequest("twitter", "tweet", "1")
//                .script(new Script("ctx._source.gender=\"male\""));
//        UpdateResponse response = client.update(updateRequest).get();

        // 使用updateRequest对象及documents进行更新
        UpdateResponse response = client.update(new UpdateRequest("twitter", "tweet", "1")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject()
                )).get();
        System.out.println(response.getIndex());
    }

    /**
     * 测试update
     * 使用updateRequest
     *
     * @throws Exception
     * @throws InterruptedException
     */
    @Test
    public void testUpdate3() throws InterruptedException, Exception {
        UpdateRequest updateRequest = new UpdateRequest("twitter1", "tweet", "1")
                .script(new Script("ctx._source.gender=\"male\""));
        UpdateResponse response = client.update(updateRequest).get();
    }

    /**
     * 测试upsert方法
     *
     * @throws Exception
     */
    @Test
    public void testUpsert() throws Exception {
        // 设置查询条件, 查找不到则添加生效
        IndexRequest indexRequest = new IndexRequest("twitter", "tweet", "2")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "qergef")
                        .field("gender", "malfdsae")
                        .endObject());
        // 设置更新, 查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("twitter", "tweet", "1")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "wenbronk")
                        .endObject())
                .upsert(indexRequest);

        client.update(upsert).get();
    }

    /**
     * 测试multi get api
     * 从不同的index, type, 和id中获取
     */
    @Test
    public void testMultiGet() {
        MultiGetResponse multiGetResponse = client.prepareMultiGet()
                .add("twitter", "tweet", "1")
                .add("twitter", "tweet", "2", "3", "4")
                .add("anothoer", "type", "foo")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetResponse) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String sourceAsString = response.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }
    }

    /**
     * bulk 批量执行
     * 一次查询可以update 或 delete多个document
     */
    @Test
    public void testBulk() throws Exception {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()));
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "another post")
                        .endObject()));
        BulkResponse response = bulkRequest.get();
        BulkItemResponse[] bulkItemResponses = response.getItems();
        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
            System.out.println(bulkItemResponse.getId());
        }
        System.out.println(response.getItems());
    }

    /**
     * 使用bulk processor
     *
     * @throws Exception
     */
    @Test
    public void testBulkProcessor() throws Exception {
        // 创建BulkPorcessor对象
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            public void beforeBulk(long paramLong, BulkRequest paramBulkRequest) {
                // TODO Auto-generated method stub
            }

            // 执行出错时执行
            public void afterBulk(long paramLong, BulkRequest paramBulkRequest, Throwable paramThrowable) {
                // TODO Auto-generated method stub
            }

            public void afterBulk(long paramLong, BulkRequest paramBulkRequest, BulkResponse paramBulkResponse) {
                // TODO Auto-generated method stub
            }
        })
                // 1w次请求执行一次bulk
                .setBulkActions(10000)
                // 1gb的数据刷新一次bulk
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                // 固定5s必须刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                // 并发请求数量, 0不并发, 1并发允许执行
                .setConcurrentRequests(1)
                // 设置退避, 100ms后执行, 最大请求3次
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        // 添加单次请求
        bulkProcessor.add(new IndexRequest("twitter", "tweet", "1"));
        bulkProcessor.add(new DeleteRequest("twitter", "tweet", "2"));

        // 关闭
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        // 或者
        bulkProcessor.close();
    }


    public static String index = "testdb1";
    public static String type = "table1";

    /**
     * 通过prepareGet方法获取指定文档信息
     */
    @Test
    public void testGet1() {
        GetResponse getResponse = client.prepareGet(index, type, "1").get();
        System.out.println(getResponse.getSourceAsString());
    }

    /**
     * prepareUpdate更新索引库中文档，如果文档不存在则会报错
     *
     * @throws IOException
     */
    @Test
    public void testUpdate1() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "will")
                .endObject();

        UpdateResponse updateResponse = client
                .prepareUpdate(index, type, "6").setDoc(source).get();

        System.out.println(updateResponse.getVersion());
    }

    /**
     * 通过prepareIndex增加文档，参数为json字符串
     */
    @Test
    public void testIndexJson() {
        String source = "{\"name\":\"will\",\"age\":18}";
        IndexResponse indexResponse = client
                .prepareIndex(index, type, "3").setSource(source).get();
        System.out.println(indexResponse.getVersion());
    }

    /**
     * 通过prepareIndex增加文档，参数为Map<String,Object>
     */
    @Test
    public void testIndexMap() {
        Map<String, Object> source = new HashMap<String, Object>(2);
        source.put("name", "Alice");
        source.put("age", 16);
        IndexResponse indexResponse = client
                .prepareIndex(index, type, "4").setSource(source).get();
        System.out.println(indexResponse.getVersion());
    }

    /**
     * 通过prepareIndex增加文档，参数为javaBean
     *
     * @throws ElasticsearchException
     * @throws JsonProcessingException
     */
    @Test
    public void testIndexBean() throws ElasticsearchException, JsonProcessingException {
        Student stu = new Student();
        stu.setName("Fresh");
        stu.setAge(22);

        IndexResponse indexResponse = client
                .prepareIndex(index, type, "5").setSource(JSON.toJSONString(stu)).get();
        System.out.println(indexResponse.getVersion());
    }

    /**
     * 通过prepareIndex增加文档，参数为XContentBuilder
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void testIndexXContentBuilder() throws IOException, InterruptedException, ExecutionException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Avivi")
                .field("age", 30)
                .endObject();
        IndexResponse indexResponse = client
                .prepareIndex(index, type, "6")
                .setSource(builder)
                .execute().get();
        //.execute().get();和get()效果一样
        System.out.println(indexResponse.getVersion());
    }

    /**
     * 通过prepareDelete删除文档
     */
    @Test
    public void testDelete1() {
        String id = "9";
        DeleteResponse deleteResponse = client.prepareDelete(index,
                type, id).get();

        System.out.println(deleteResponse.getVersion());

//        //删除所有记录
//        transportClient.prepareDeleteByQuery(index).setTypes(type)
//                .setQuery(QueryBuilders.matchAllQuery()).get();
    }

    /**
     * 删除索引库，不可逆慎用
     */
    @Test
    public void testDeleteeIndex() {
        client.admin().indices().prepareDelete("shb01", "shb02").get();
    }

    /**
     * 求索引库文档总数
     */
    @Test
    public void testCount()
    {
//        long count = client.prepareCount(index).get().getCount();
//        System.out.println(count);
    }

    /**
     * 通过prepareBulk执行批处理
     *
     * @throws IOException
     */
    @Test
    public void testBulk1() throws IOException {
        //1:生成bulk
        BulkRequestBuilder bulk = client.prepareBulk();

        //2:新增
        IndexRequest add = new IndexRequest(index, type, "10");
        add.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Henrry").field("age", 30)
                .endObject());

        //3:删除
        DeleteRequest del = new DeleteRequest(index, type, "1");

        //4:修改
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("name", "jack_1").field("age", 19).endObject();
        UpdateRequest update = new UpdateRequest(index, type, "2");
        update.doc(source);

        bulk.add(del);
        bulk.add(add);
        bulk.add(update);
        //5:执行批处理
        BulkResponse bulkResponse = bulk.get();
        if (bulkResponse.hasFailures()) {
            BulkItemResponse[] items = bulkResponse.getItems();
            for (BulkItemResponse item : items) {
                System.out.println(item.getFailureMessage());
            }
        } else {
            System.out.println("全部执行成功！");
        }
    }

    /**
     * 通过prepareSearch查询索引库
     * setQuery(QueryBuilders.matchQuery("name", "jack"))
     * setSearchType(SearchType.QUERY_THEN_FETCH)
     */
    @Test
    public void testSearch() {
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()) //查询所有
//                .setQuery(QueryBuilders.matchQuery("name", "tom").operator(Operator.AND)) //根据tom分词查询name,默认or
                .setQuery(QueryBuilders.multiMatchQuery(18, "name", "age")) //指定查询的字段
//                .setQuery(QueryBuilders.queryStringQuery("name:jack* AND age:[0 TO 19]")) //根据条件查询,支持通配符大于等于0小于等于19
                //相同的查询字段后面的条件会覆盖前面的条件
//                .setQuery(QueryBuilders.termQuery("name", "tom"))//查询时不分词
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setFrom(0).setSize(10)//分页
                .addSort("age", SortOrder.DESC)//排序
                .get();

        SearchHits hits = searchResponse.getHits();
        long total = hits.getTotalHits();
        System.out.println(total);
        SearchHit[] searchHits = hits.hits();
        for (SearchHit s : searchHits) {
            System.out.println(s.getSourceAsString());
        }
    }

    @Test
    public void testSearch1() {

        SearchResponse response = client.prepareSearch("blog", "twitter")
                .setTypes("article", "tweet")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("title", "test"))                 // Query
                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        SearchHits hits = response.getHits();
        long total = hits.getTotalHits();
        System.out.println(total);
        SearchHit[] searchHits = hits.hits();
        for (SearchHit s : searchHits) {
            System.out.println(s.getSourceAsString());
        }

    }

    /**
     * 多索引，多类型查询
     * timeout
     */
    @Test
    public void testSearchsAndTimeout() {
        SearchResponse searchResponse = client.prepareSearch("shb01", "shb02").setTypes("stu", "tea")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setTimeout(TimeValue.MINUS_ONE)
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println(totalHits);
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit h : hits2) {
            System.out.println(h.getSourceAsString());
        }
    }

    /**
     * 过滤，
     * lt 小于
     * gt 大于
     * lte 小于等于
     * gte 大于等于
     */
//    @Test
//    public void testFilter() {
//        SearchResponse searchResponse = client.prepareSearch(index)
//                .setTypes(type)
//                .setQuery(QueryBuilders.matchAllQuery()) //查询所有
//                .setSearchType(SearchType.QUERY_THEN_FETCH)
////              .setPostFilter(FilterBuilders.rangeFilter("age").from(0).to(19)
////                      .includeLower(true).includeUpper(true))
//                .setPostFilter(FilterBuilders.rangeFilter("age").gte(18).lte(22))
//                .setExplain(true) //explain为true表示根据数据相关度排序，和关键字匹配最高的排在前面
//                .get();
//
//
//        SearchHits hits = searchResponse.getHits();
//        long total = hits.getTotalHits();
//        System.out.println(total);
//        SearchHit[] searchHits = hits.hits();
//        for (SearchHit s : searchHits) {
//            System.out.println(s.getSourceAsString());
//        }
//    }

    /**
     * 高亮
     */
//    @Test
//    public void testHighLight()
//    {
//        SearchResponse searchResponse = client.prepareSearch(index)
//                .setTypes(type)
//                //.setQuery(QueryBuilders.matchQuery("name", "Fresh")) //查询所有
//                .setQuery(QueryBuilders.queryStringQuery("name:F*"))
//                .setSearchType(SearchType.QUERY_THEN_FETCH)
//                .addHighlightedField("name")
//                .setHighlighterPreTags("<font color='red'>")
//                .setHighlighterPostTags("</font>")
//                .get();
//
//
//        SearchHits hits = searchResponse.getHits();
//        System.out.println("sum:" + hits.getTotalHits());
//
//        SearchHit[] hits2 = hits.getHits();
//        for(SearchHit s : hits2)
//        {
//            Map<String, HighlightField> highlightFields = s.getHighlightFields();
//            HighlightField highlightField = highlightFields.get("name");
//            if(null != highlightField)
//            {
//                Text[] fragments = highlightField.fragments();
//                System.out.println(fragments[0]);
//            }
//            System.out.println(s.getSourceAsString());
//        }
//    }

    /**
     * 分组
     */
//    @Test
//    public void testGroupBy()
//    {
//        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type)
//                .setQuery(QueryBuilders.matchAllQuery())
//                .setSearchType(SearchType.QUERY_THEN_FETCH)
//                .addAggregation(AggregationBuilders.terms("group_age")
//                        .field("age").size(0))//根据age分组，默认返回10，size(0)也是10
//                .get();
//
//        Terms terms = searchResponse.getAggregations().get("group_age");
//        List<Bucket> buckets = terms.getBuckets();
//        for(Bucket bt : buckets)
//        {
//            System.out.println(bt.getKey() + " " + bt.getDocCount());
//        }
//    }

    /**
     * 聚合函数,本例之编写了sum，其他的聚合函数也可以实现
     *
     */
//    @Test
//    public void testMethod()
//    {
//        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type)
//                .setQuery(QueryBuilders.matchAllQuery())
//                .setSearchType(SearchType.QUERY_THEN_FETCH)
//                .addAggregation(AggregationBuilders.terms("group_name").field("name")
//                        .subAggregation(AggregationBuilders.sum("sum_age").field("age")))
//                .get();
//
//        Terms terms = searchResponse.getAggregations().get("group_name");
//        List<Bucket> buckets = terms.getBuckets();
//        for(Bucket bt : buckets)
//        {
//            Sum sum = bt.getAggregations().get("sum_age");
//            System.out.println(bt.getKey() + "  " + bt.getDocCount() + " "+ sum.getValue());
//        }
//
//    }


}