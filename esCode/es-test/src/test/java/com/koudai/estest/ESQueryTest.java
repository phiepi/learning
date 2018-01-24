package com.koudai.estest;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by liupinghe.
 */
public class ESQueryTest {

    private TransportClient transportClient;


    public static String index = "testdb1";
    public static String type = "table1";


    @Before
    public void before() throws Exception {

        ClientFactory clientFactory = new ClientFactory();
        clientFactory.setEsClusterName("my-application");
        clientFactory.setEsServerIps("10.8.96.11");
        clientFactory.setEsServerPort(9300);
        transportClient = clientFactory.getTransPortClient();

    }

    /**
     * search查询详解
     *
     * @throws Exception
     */
    @Test
    public void test18() throws Exception {
        SearchResponse searchResponse = transportClient.prepareSearch(index)//指定索引库
                .setTypes(type)//指定类型
                /**
                 * QUERY_AND_FETCH：只追求查询性能的时候可以选择
                 * QUERY_THEN_FETCH：默认
                 * DFS_QUERY_AND_FETCH：只需要排名准确即可
                 * DFS_QUERY_THEN_FETCH：对效率要求不是非常高，对查询准确度要求非常高，建议使用这一种
                 */
                .setSearchType(SearchType.DEFAULT)
                /**
                 * matchQuery模糊查询，只要name包含"tom"都会查询出来
                 */
//                .setQuery(QueryBuilders.matchQuery("name", "tom"))//指定查询条件,这里不支持通配符 * ？
//                .setQuery(QueryBuilders.matchAllQuery())//查询所有
                /**
                 * multiMatchQuery("tom", "name","title")查询
                 *                   　等价于SQL语句，就是，where name=tom or title=tom
                 *                   　*比如是文章数据。标题，描述，正文。把所有这些文章数据都查出来。
                 */
//                .setQuery(QueryBuilders.multiMatchQuery("tom", "name","title"))//支持一个值同时匹配多个字段
//                .setQuery(QueryBuilders.queryStringQuery("name:to?"))//支持lucene的语法 AND OR,通配符 * ？ 如果对lucene比较熟悉，或者是想用通配符，可以使用这个
//                .setQuery(QueryBuilders.queryStringQuery("name:jack* AND age:[0 TO 19]")) //根据条件查询,支持通配符大于等于0小于等于19
                /**
                 * must、should、mustNot
                 *这里，must类似于and。should类似于or
                 */
//        GET /my_store/products/_search
//        {
//            "query" : {
//            "filtered" : {
//                "filter" : {
//                    "bool" : {
//                        "should" : [
//                        { "term" : {"productID" : "KDKE-B-9947-#kL5"}},
//                        { "bool" : {
//                            "must" : [
//                            { "term" : {"productID" : "JODL-X-1937-#pV7"}},
//                            { "term" : {"price" : 30}}
//                  ]
//                        }}
//              ]
//                    }
//                }
//            }
//        }
//        }
                .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("name", "tomc123"))
                        .filter(QueryBuilders.boolQuery()
                                        .should(QueryBuilders.termQuery("score", "1"))
                                        .should(QueryBuilders.termQuery("score", "2"))
//                                .filter(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("score", "2")))
                        )
                )

//                        .must(QueryBuilders.matchQuery("age", 20)))//组合查询，支持多个查询条件,并且可以给查询条件设置权重
//                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name", "tom"))
//                        .mustNot(QueryBuilders.matchQuery("age", 16)))//组合查询，支持多个查询条件,并且可以给查询条件设置权重
//                .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name", "jack"))
//                        .should(QueryBuilders.matchQuery("age", 19)))//组合查询，支持多个查询条件,并且可以给查询条件设置权重
                /**
                 * name权重为8.0f>age权重1.0f，优先返回name="jack"的数据
                 */
//                .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name", "jack").boost(8.0f))
//                        .should(QueryBuilders.matchQuery("age", 19).boost(1.0f)))//组合查询，支持多个查询条件,并且可以给查询条件设置权重
                /**
                 * matchQuery 是模糊查询，termQuery是精确查询
                 */
//                .setQuery(QueryBuilders.termQuery("desc", "test"))//精确查询主要针对人名还有地名
                //注意：一般需要精确查询的字段，在存储的时候都不建议分词。但是已经分词了，还想精确精确查询，使用queryStringQuery，在需要精确查询的词语外面使用双引号引起来
//                .setQuery(QueryBuilders.queryStringQuery("name:\"jack hello\""))
                //AND的意思是 返回的数据中必须包含 你好  和  中国这两个词  OR的意思是只包含一个词即可
//                .setQuery(QueryBuilders.matchQuery("name", "jack hello").operator(Operator.AND))
                //实现分页,分页参数
                .setFrom(0)
                .setSize(100)
                //排序,根据某一个字段排序
//                .addSort("age", SortOrder.DESC)//ASC是升序，DESC是倒序

                /**
                 * lt：小于
                 * lte：小于等于
                 * gt：大于
                 * gte：大于等于
                 */

                //过滤,默认是闭区间
                // 10=<x<=20这个过滤条件可以缓存，多次查询效率高
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(18).to(19).includeLower(true).includeUpper(true))
//                .setPostFilter(QueryBuilders.rangeQuery("age").gte(18).lte(19))
//                .setPostFilter(QueryBuilders.rangeQuery("age").gte(18).lt(19))
//                .setPostFilter(QueryBuilders.rangeQuery("age").gt(17).lt(19))
                //注意：我的版本是es2.4.3,     因为es1.x 是用filterbuilders      es2.x 使用querybuilders
                .setExplain(true)//按照查询数据的匹配度返回数据
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数：" + totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        System.out.println("总数2：" + hits2.length);//totalHits 和hits2.length 获取的长度有什么区别
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }


    /**
     * search查询详解
     *
     * @throws Exception
     *
     * 关于查询可以定制一个 setQuery(QueryBuilder) 和 setPostFilter(filter) 。当然还可以根据需要添加排序，分页等参数。这里主要谈一下query和filter.
    query的作用是根据条件创建搜索规则。
    postFilter的作用是在搜索的结果的基础上过滤结果。
    elasticSearch 2.4 以后没有filter相关概念，所以构建query和filter都用QueryBuilder。简单举几个例子

    QueryBuilder query = QueryBuilders.matchAllQuery(); //全部查询
    QueryBuilder query = QueryBuilders.queryStringQuery(key);

    //filter
    QueryBuilder  timeFilter = QueryBuilders.rangeQuery("doc.create_time").from(begin_time).to(end_time);
    QueryBuilder  channelFilter = QueryBuilders.termsQuery("doc.channel", channel_list);
    elasticsearch提供了boolQuery来代替filter的or, and, not等操作。如果想要把上面的filter关联起来需要用must, mustNot, should等操作。

    filter = QueryBuilders.boolQuery().must(timeFilter).must(channelFilter);
    must可以理解为and, mustNot可理解为not, 但should不能理解为or，这个对查询的结果相关性有影响。
    elasticsearch post filter可以作简单的yes/not过滤，但是无法对复查结果进行过滤，至少我没发现。比如下面这种情况。

    我有两种类型的文档，type =1/2；
    我不要两天以外的type=1的文档；
    QueryBuilder typeFilter = QueryBuilders.termQuery("doc.media_type", 2);
    QueryBuilder timeFilter = QueryBuilders.rangeQuery("doc.create_time").lt(TimeUtil.getDaysBefore(2));

    QueryBuilder wFilters = QueryBuilders.boolQuery().must(typeFilter).must(timeFilter);

    filter = QueryBuilders.boolQuery().mustNot(wChatFilters);
    这种是没办法做到的，这样只会过滤掉所有type=2的文档以及所有两天以外的文档。
    继续探索。。。


    termQuery、matchQuery、rangeQuery
    ES 中可以使用 Query DSL 来完整定义一个查询请求，请求主体由两类语句组成：叶子查询子句（Leaf query clauses）与复合查询子句（Compound query clauses）。其中 Leaf query clauses 主要用于从特定的字段中搜索目标内容，对应的 Query 类型包含：match, term, range。

    这里我们先忽略 Compound query，以后我们会再提到复合查询。现在我们先关注 Leaf query，显然其包含的三种 Query 类型分别对应 QueryBuilders 类的 matchQuery, termQuery, rangeQuery。对此我们还需要注意的一点是，这三种查询在使用时只能针对某一个字段进行操作。

    termQuery、matchQuery、rangeQuery
    接着上面，我们需要了解这三种查询类型的作用与区别。
    (1)rangeQuery
    rangeQuery 是对字段的值进行范围查询，这个比较容易理解。

    (2)matchQuery
    match 会根据给定的关键词对指定字段进行搜索，特别的是，ES 会将用户给定的关键字进行分词，然后按分词结果进行分别搜索，根据相关度等设置对所有分词的查询结果进行评分排序，最终汇总成最终结果返回。例如"match" : { "desc" : "查询新税时间" }，分词结果可能为 "查询新税时间"、"查询"、"新税"、"时间"，显然这样搜索返回的结果中必然有很多记录并非我们需要的。

    (3)termQuery
    term 也可根据给定的关键词对指定字段进行搜索，与 match 截然不同的是，term 是严格匹配，即指定字段的内容与给定关键词完全一致时才算匹配。就以我们上面【尝试搜索 - 2-Query】提到的那条记录为例，若我们查询语句为"term" : { "desc" : "查询新税" }，是无法成功匹配的。

    另外，关于 term 还需特别注意的是，<font style="color:red">term 仅能作用于 index 状态为 analyzed 的字段，若在 indices-templates 中设置该字段为 not_analyzed，那即使该字段内容与关键词完全一致，ES 也不会认定为匹配。</font> 关于 templates，详细请看我之前的博文：多系统接入 ELK(3)
     */
    @Test
    public void test19() throws Exception {

        QueryBuilder  timeFilter = QueryBuilders.rangeQuery("doc.create_time").from("begin_time").to("end_time");
        QueryBuilder channelFilter = QueryBuilders.termsQuery("doc.channel", "");
        SearchResponse searchResponse = transportClient.prepareSearch(index)//指定索引库
                .setTypes(type)//指定类型
                .setSize(1000)
//                .setQuery(QueryBuilders.matchQuery("name", "tom"))//指定查询条件,这里不支持通配符 * ？
//                .setQuery(QueryBuilders.matchAllQuery())//查询所有
//                .setQuery(QueryBuilders.multiMatchQuery("tom", "name","title"))//支持一个值同时匹配多个字段
//                .setQuery(QueryBuilders.queryStringQuery("name:to?"))//支持lucene的语法 AND OR,通配符 * ？ 如果对lucene比较熟悉，或者是想用通配符，可以使用这个
                .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.matchQuery("name", "jack123"))

//                        .mustNot(QueryBuilders.matchQuery("age", 16))
                )//组合查询，支持多个查询条件,并且可以给查询条件设置权重
                .setQuery(QueryBuilders.boolQuery()
                .should(QueryBuilders.matchQuery("math", "10"))
                .should(QueryBuilders.matchQuery("math", "20")))
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数：" + totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        System.out.println("总数2：" + hits2.length);//totalHits 和hits2.length 获取的长度有什么区别
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }


    /**
     * 分组求count
     *
     * @throws Exception
     */
    @Test
    public void test20() throws Exception {
        SearchResponse searchResponse = transportClient.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).setSearchType(SearchType.DEFAULT)
                //添加分组字段
                .addAggregation(AggregationBuilders.terms("term_age").
                        field("age"))//设置为0默认会返回所有分组的数据
                .get();
        //获取分组信息
        Terms terms = searchResponse.getAggregations().get("term_age");
        System.out.println(terms.getBuckets().size());
        for (Terms.Bucket entry : terms.getBuckets()) {
            Long key = (Long) entry.getKey(); // bucket key
            long docCount = entry.getDocCount(); // Doc count
            System.out.println(key + "_" + docCount);

//            // We ask for top_hits for each bucket
//            TopHits topHits = entry.getAggregations().get("term_age");
//            for (SearchHit hit : topHits.getHits().getHits()) {
//                System.out.println(" -> id " + hit.getId() + " _source [{}]"
//                        + hit.getSource().get("category_name"));
//            }
        }

    }

    /**
     * 分组求sum，统计每个学员的总成绩
     *
     * @throws Exception
     */
    @Test
    public void test21() throws Exception {
        SearchResponse searchResponse = transportClient.prepareSearch(index).setTypes(type)
//                .setQuery(QueryBuilders.matchAllQuery())
                .setQuery(QueryBuilders.matchQuery("name", "tom").operator(Operator.AND))
                //添加分组字段
                .addAggregation(AggregationBuilders.terms("").field("name")
                        .subAggregation(AggregationBuilders.sum("").field("score")))
                .get();
        Terms terms = searchResponse.getAggregations().get("");
        for (Terms.Bucket entry : terms.getBuckets()) {
            Sum sum = entry.getAggregations().get("");
            System.out.println(entry.getKey() + "--" + sum.getValue());
        }
    }

    /**
     * 多索引库多类型查询
     * @throws Exception
     */
    @Test
    public void test22() throws Exception {
        SearchResponse searchResponse = transportClient.prepareSearch("zhouls*")//指定一个或者多个索引库,支持通配符
//                SearchResponse searchResponse = transportClient.prepareSearch("zhouls,love,liuch")//指定一个或者多个索引库,支持通配符


                .setTypes(type)//指定一个或者多个类型,但不支持通配符
//                .setTypes("emp","emp1")//指定一个或者多个类型,但不支持通配符


                .setQuery(QueryBuilders.matchQuery("name", "tom hehe"))//指定查询条件
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)//指定查询方式
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数："+totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }

    }

    @Test
    public void test23() throws Exception {

        QueryStringQueryBuilder  queryStringBuilder= new QueryStringQueryBuilder("tom").field("name");
        queryStringBuilder.useDisMax(true);
        queryStringBuilder.field("name",8);

        SearchResponse searchResponse = transportClient.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.DEFAULT)
                .setQuery(queryStringBuilder)
                .setFrom(0)
                .setSize(10)
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数：" + totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        System.out.println("总数2：" + hits2.length);//totalHits 和hits2.length 获取的长度有什么区别
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }







}
