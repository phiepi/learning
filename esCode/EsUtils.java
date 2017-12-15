package com.koudai.common.utils;


import com.koudai.common.enums.OperateState;
import com.koudai.common.vo.CrashInfo;
import com.koudai.common.vo.ReportBean;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.joda.time.DateTimeUtils;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by zhangzhang on 16/4/28.
 */
public class EsUtils {
    /**
     * esClientPool大小
     */
    private static final int POOL_SIZE = 20;

    public volatile static EsClientPool esClientPool;

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Client getClient(int port, String clusterName, String ... ips){
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        TransportClient client = new TransportClient(settings);
        for(String ip : ips){
            client.addTransportAddress(new InetSocketTransportAddress(ip, port));
        }

        return client;
    }

    public static Client getTransportClient(int port, String clusterName, String ... ips){
        esClientPool = EsClientPool.getInstance(clusterName, POOL_SIZE, ips, port);
        return esClientPool.borrowClient();
    }

    public static void close(Client client){
        if(esClientPool != null){
            esClientPool.close(client);
        }
    }

    public static void addIndex(Client client){
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("name", "zhangzhang")
                    .field("age", 26)
                    .field("sex", "m").endObject()
                    .startObject().field("address", "河北省武安市西寺庄乡东寺庄村").field("type", "string").field("indexAnalyzer", "ik").field("searchAnalyzer", "ik").endObject()
                    .startObject().field("desc", "他黑黑的脸上，露出一排白玉似的牙齿 ").field("type", "string").field("indexAnalyzer", "ik").field("searchAnalyzer", "ik")
                    .endObject();
            IndexResponse response = client.prepareIndex("employee", "emp", "3").setSource(builder).execute().actionGet();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addComplexIndex(Client client){
        String[] versions = {"1.1", "1.2", "3.4", "7.2"};
        long millis = System.currentTimeMillis();
        int[] millAry = {80000000, 980550000,490055000, 84000000, 54000000 ,234340344};
        String[] types = {"info", "error", "debug", "warn"};
        String[] appIds = {"qq", "wechat", "uc"};
        String[] errorType = {"NullPointerException", "IllegalArgumentException", "UnsupportedOperationException", "IllegalStateException", "IndexOutOfBoundsException"};
        String[] desc = {"Unless required by applicable law or agreed to in writing",
                "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND",
                "either express or implied. See the License for the specific language governing permissions and limitations under the License"};

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Random random = new Random();
        for(int i = 200; i< 300; i++){
            CrashInfo crashInfo = new CrashInfo();
            ReportBean bean = new ReportBean();
            bean.setCrashInfo(crashInfo);
            crashInfo.setDesc(desc[random.nextInt(desc.length)]);
            crashInfo.setErrorType(errorType[random.nextInt(errorType.length)]);
            bean.setVersion(versions[random.nextInt(versions.length)]);
            bean.setAppid(appIds[random.nextInt(appIds.length)]);
            bean.setType(types[random.nextInt(types.length)]);
            bean.setProxy_timestamp(millis - random.nextInt(1000000000));
            bean.setSuid(random.nextInt(5));

            IndexRequestBuilder indexRequestBuilder = client.prepareIndex("employee", "test9", String.valueOf(i)).setSource(JSONUtil.toJSON(bean));
            bulkRequestBuilder.add(indexRequestBuilder);
            System.out.println(JSONUtil.toJSON(bean));
        }
        BulkResponse responses = bulkRequestBuilder.execute().actionGet();

        if(responses.hasFailures()){
            System.out.println(responses.buildFailureMessage());
        }
    }

    public static void addMultIndex(Client client) throws IOException {
        String[] versions = {"1.1", "1.2", "3.4", "7.2"};
        long millis = System.currentTimeMillis();
        int[] millAry = {80000000, 980550000,490055000, 84000000, 54000000 ,234340344};
        String[] errorType = {"NullPointerException", "IllegalArgumentException", "UnsupportedOperationException", "IllegalStateException", "IndexOutOfBoundsException"};
        String[] desc = {"Unless required by applicable law or agreed to in writing",
                         "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND",
                        "either express or implied. See the License for the specific language governing permissions and limitations under the License"};
        Random random = new Random();
        for(int i = 0; i < 100; i++){
            //System.out.println(simpleDateFormat.format(new Date(millis - millAry[random.nextInt(millAry.length)])));
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("version", versions[random.nextInt(versions.length)]).endObject()
                    .startObject().field("date", simpleDateFormat.format(new Date(millis - millAry[random.nextInt(millAry.length)]))).field("type", "date").field("index", "not_analyzed")
                    .field("errorType", errorType[random.nextInt(errorType.length)])
                    .field("desc", desc[random.nextInt(desc.length)]).endObject();
//            Map<String, Object> json = new HashMap<String, Object>();
//            json.put("version", versions[random.nextInt(versions.length)]);
//            json.put("date", new Date(millis - millAry[random.nextInt(millAry.length)]));
//            json.put("errorType", errorType[random.nextInt(errorType.length)]);
//            json.put("desc", desc[random.nextInt(desc.length)]);
            System.out.println(builder.string());
            IndexResponse response = client.prepareIndex("employee", "stack", String.valueOf(i)).setSource(builder).execute().actionGet();
        }

    }

    public static void get(Client client){
        GetResponse response = client.prepareGet("employee", "emp", "3").execute().actionGet();
        System.out.println(response.getSourceAsString());
    }

    public static void search(Client client){
        SearchResponse response = client.prepareSearch("employee").setTypes("bugly")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("message.platform", "android")).execute().actionGet();
        System.out.println(response.toString());
    }

    public static void analyze(Client client){

        AnalyzeRequestBuilder request = new AnalyzeRequestBuilder(client.admin().indices(), "employee", "东寺庄");
        request.setTokenizer("ik");
        List<AnalyzeResponse.AnalyzeToken> listAnalysis = request.execute().actionGet().getTokens();
        for( AnalyzeResponse.AnalyzeToken term : listAnalysis) {


            SearchResponse response = client.prepareSearch("employee")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("address", term.getTerm()))).execute().actionGet();
            System.out.println(response.toString());
            System.out.println(term.getTerm() + "=========================");
        }


    }



    public static void aggregation(Client client){
        SearchResponse response = client.prepareSearch("employee")
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("version", "3.4")))
                .setFrom(0).setSize(20)
                //显示指定的字段
                //.addField("errorType")
                .addHighlightedField("version")
                .addField("errorType").addField("date").addField("version")
                //.must(QueryBuilders.matchQuery("errorType", "NullPointerException")))
                .addAggregation(AggregationBuilders.terms("count").field("errorType"))
                .addFacet(FacetBuilders.termsFacet("f").field("date"))
                .execute().actionGet();
        System.out.println(response.toString());
        //提取高亮文本
//        SearchHits hits = response.getHits();
//        for(SearchHit hit : hits){
//            Map<String, HighlightField> result = hit.highlightFields();
//            System.out.println("A map of highlighted fields:" + result);
//            HighlightField titleField = result.get("version");
//            Text[] titleTexts =  titleField.fragments();
//            for(Text text : titleTexts){
//                System.out.println("version text: "+text);
//            }
//        }

    }


    public static void scroll(Client client){

    }

    /**
     * 批量插入
     * @param client
     */
    public static void bulk(Client client) throws IOException {
        String[] versions = {"1.1", "1.2", "3.4", "7.2"};
        long millis = System.currentTimeMillis();
        int[] millAry = {80000000, 980550000,490055000, 84000000, 54000000 ,234340344};
        String[] types = {"info", "error", "debug", "warn"};
        String[] appIds = {"qq", "wechat", "uc"};
        String[] errorType = {"NullPointerException", "IllegalArgumentException", "UnsupportedOperationException", "IllegalStateException", "IndexOutOfBoundsException"};
        String[] desc = {"Unless required", "WITHOUT WARRANTIES", "either express"};

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Random random = new Random();
        for(int i = 200; i< 300; i++){
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("version", versions[random.nextInt(versions.length)])
                    .field("proxy_timestamp", millis - random.nextInt(1000000000))
                    .field("errorType", errorType[random.nextInt(errorType.length)])
                    .field("suid", random.nextInt(5))
                    .field("appid", appIds[random.nextInt(appIds.length)])
                    .field("type", types[random.nextInt(types.length)])
                    .field("desc", desc[random.nextInt(desc.length)]).endObject();
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex("employee", "test8", String.valueOf(i)).setSource(builder);
            bulkRequestBuilder.add(indexRequestBuilder);
            System.out.println(builder.string());
        }
        BulkResponse responses = bulkRequestBuilder.execute().actionGet();

        if(responses.hasFailures()){
            System.out.println(responses.buildFailureMessage());
        }
    }

    public static void bulkFormal(Client client) throws IOException{
        String[] versions = {"1.1", "2.2", "3.4", "7.1"};
        long millis = System.currentTimeMillis();
        int[] millAry = {80000000, 980550000,490055000, 84000000, 54000000 ,234340344};
        //String[] types = {"javaCrash", "jsCrash", "luaCrash"};
        String[] types = {"javaCrash", "jsCrash", "luaCrash"};
        String[] appIds = {"qq", "wechat", "uc"};
        String[] errorType = {"NullPointerException", "IllegalArgumentException", "UnsupportedOperationException", "IllegalStateException", "IndexOutOfBoundsException"};
        String[] desc = {"Unless required", "WITHOUT WARRANTIES", "either express"};
        String[] machineNames = {"oppo", "xiaomi", "vivo"};
        String[] channels = {"miMarket", "360Market", "91Market", "yingyongbao"};
        String[] systems = {"4.3", "5.5"};
        String[] methods = {"com.a.b.func1", "com.a.c.func2", "com.a.d.func3"};
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Random random = new Random();
        FileWriter fw = new FileWriter("/Users/zhangzhang/script/json1");
        BufferedWriter  writer = new BufferedWriter(fw);
        for(int i = 1; i< 10000; i++) {
            String crashType = errorType[random.nextInt(errorType.length)];
            String method = methods[random.nextInt(methods.length)];
            String crashInfo = desc[random.nextInt(desc.length)];
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .field("suid", random.nextInt(50))
                    .field(Constants.MACHINENAME, machineNames[random.nextInt(machineNames.length)])
                    .field("proxy_timestamp", millis - random.nextInt(1000000000))
                    .field("sdk_version", versions[random.nextInt(versions.length)])
                    .field("version", versions[random.nextInt(versions.length)])
                    .field("appid", appIds[random.nextInt(appIds.length)])
                    .field("errorType", crashType + "-" + method + "-" + crashInfo)
                    .field("mid", "android")
                    .field("channel", channels[random.nextInt(channels.length)])
                    .field("isJailBroken", random.nextInt(1))
                    .field("report", desc[random.nextInt(desc.length)])
                    .field("type", types[random.nextInt(types.length)])
                    .field("system", systems[random.nextInt(systems.length)])
                    .field("timestamp", millis - random.nextInt(1000000000))
                    .field("crashType", crashType)
                    .field("keymethod", method)
                    .field("crashinfo", crashInfo)
                    .field("memory_space", random.nextInt(100000000))
                    .field("memory_space_avail", random.nextInt(10000000))
                    .field("disk_space", random.nextInt(100000000))
                    .field("disk_space_avail", random.nextInt(10000000))
                    .field("cpu_used", random.nextDouble())
                    .field(Constants.OPERATETYPE, 0)
                    .field(Constants.REPORT_TYPE, random.nextInt(2)+1)
                    .endObject();
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex("employee", "bugly", String.valueOf(i)).setSource(builder);
            bulkRequestBuilder.add(indexRequestBuilder);
            //System.out.println(builder.string());

            writer.write(builder.string());
            writer.newLine();
        }
        writer.flush();
        writer.close();
        BulkResponse responses = bulkRequestBuilder.execute().actionGet();
    }

    public static void searchAll(Client client){
        SearchResponse response = client.prepareSearch("employee")
                .setQuery(QueryBuilders.boolQuery()//.must((QueryBuilders.matchQuery("version", "3.4")))
                .must(QueryBuilders.filteredQuery(QueryBuilders.rangeQuery("proxy_timestamp").from(1461716119722L).to(1471724142552L), null)))
                //.setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(AggregationBuilders.terms("count").field("crashInfo.errorType")
                        .subAggregation(AggregationBuilders.terms("versionCount").field("version")
                                .subAggregation(AggregationBuilders.cardinality("uvPerVersion").field("suid"))
                                .subAggregation(AggregationBuilders.max("recent").field("proxy_timestamp"))
                                .subAggregation(AggregationBuilders.min("latest").field("proxy_timestamp")))
                        .subAggregation(AggregationBuilders.cardinality("uv").field("suid")))
                .setTypes("test9").execute().actionGet();

//        for(SearchHit hit : response.getHits().hits()){
//            System.out.println("===========================");
//            System.out.println(hit.getSourceAsString());
//
//            System.out.println("===========================");
//        }
        System.out.println(response);

        Map<String, Aggregation> aggMap = response.getAggregations().asMap();
        StringTerms count = (StringTerms) aggMap.get("count");
        Iterator<Terms.Bucket> countItr = count.getBuckets().iterator();
        while(countItr.hasNext()){
            Terms.Bucket bucket = countItr.next();
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());

            StringTerms versionCount = (StringTerms) bucket.getAggregations().asMap().get("versionCount");
            Iterator<Terms.Bucket> versionCountItr = versionCount.getBuckets().iterator();

            while(versionCountItr.hasNext()){
                Terms.Bucket bk = versionCountItr.next();
                System.out.println(bk.getKey() + ":" + bk.getDocCount() + "==========");
                Map<String, Aggregation> map = bk.getAggregations().asMap();
                Min latest = (Min) map.get("latest");
                Max recent = (Max) map.get("recent");
                Cardinality card = (Cardinality) map.get("uvPerVersion");

                System.out.println(latest.getValue());
                System.out.println(card.getValue());
            }
        }

        //System.out.println(aggMap.get("count"));
    }
    public static void update(Client client) throws IOException, ExecutionException, InterruptedException {
        //批量更新状态
        List<String> idList = new ArrayList<String>();
        idList.add("AVUJt6YwA75d83yG7aws");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (String id : idList) {
            UpdateRequest updateRequest = new UpdateRequest("bugly-2016-06-01", "crash", id)
                    .doc(XContentFactory.jsonBuilder()
                            .startObject().field("@message").startObject()
                            .field(Constants.OPERATETYPE, OperateState.OPERATED.getCode())
                            .endObject().endObject());
            bulkRequestBuilder.add(updateRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
        System.out.println(bulkResponse.buildFailureMessage());

    }

    public static void histogram(Client client){
        String[] versions = {"0.0.1", "0.0.2", "0.0.8", "all"};
        try{
//            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(Constants.APPID, "weidian.GLStatisticalAnalysisDemo"))
//                    .must(QueryBuilders.matchQuery(Constants.REPORT_TYPE, 1))
//                    .must(QueryBuilders.rangeQuery(Constants.PROXY_TIMESTAMP).from(0).to(System.currentTimeMillis()));
//            for (String appVersion : versions) {
//                queryBuilder.should(QueryBuilders.matchQuery(Constants.VERSION, appVersion));
//            }
//            SearchRequestBuilder searchQueryBuilder = client.prepareSearch("bugly").setTypes("crash")
//                    .setQuery(queryBuilder)
//                    .addField(Constants.PROXY_TIMESTAMP)
//                    .addField(Constants.SUID)
//                    .addField(Constants.VERSION)
//                    .addAggregation(AggregationBuilders.dateHistogram("period").field(Constants.PROXY_TIMESTAMP).interval(DateHistogram.Interval.DAY)
//                            .subAggregation(AggregationBuilders.terms("versionCount").field(Constants.VERSION)
//                                    .subAggregation(AggregationBuilders.cardinality("uvPerVersion").field(Constants.SUID))
//                                    .subAggregation(AggregationBuilders.terms("crashNumPerUser").field(Constants.SUID).minDocCount(2)))
//                            .subAggregation(AggregationBuilders.cardinality("uv").field(Constants.SUID))
//                            .subAggregation(AggregationBuilders.terms("allCrashNumPerUser").field(Constants.SUID).minDocCount(2)));
//            SearchResponse response = searchQueryBuilder.execute().actionGet();
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(Constants.APPID, "weidian.GLStatisticalAnalysisDemo"))
                    .must(QueryBuilders.matchQuery(Constants.REPORT_TYPE, 1))
                    .must(QueryBuilders.rangeQuery(Constants.PROXY_TIMESTAMP).from(0).to(System.currentTimeMillis()))
                    .must(QueryBuilders.rangeQuery(Constants.UPTIME).from(1000).to(Long.MAX_VALUE));
            SearchRequestBuilder searchQueryBuilder = client.prepareSearch("bugly").setTypes("crash")
                    .setQuery(queryBuilder)
                    .addField(Constants.PROXY_TIMESTAMP)
                    .addAggregation(AggregationBuilders.dateHistogram("period").field(Constants.PROXY_TIMESTAMP).interval(DateHistogram.Interval.DAY)
                            .subAggregation(AggregationBuilders.terms("versionCount").field(Constants.VERSION)
                                    .subAggregation(AggregationBuilders.cardinality("uvPerVersion").field(Constants.SUID))));
            SearchResponse response = searchQueryBuilder.execute().actionGet();
            System.out.println(response);
        }finally {
            client.close();
        }
    }

    public static void matchAll(Client client){
        try{
            SearchResponse response = client.prepareSearch("bugly").setTypes("crash")
                    .setQuery(QueryBuilders.matchQuery(Constants.APPID, "com.koudai.weishop"))
                    .setFrom(0).setSize(100)
                    .execute().actionGet();
            System.out.println(response);
        }finally {
            client.close();
        }
    }

    public static void searchOne(Client client){
        try {
            QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(Constants.APPID, "com.example.chenpengpeng.tongjidemo2"))
                    .must(QueryBuilders.matchQuery(Constants.REPORT_TYPE, 1))
                    .must(QueryBuilders.matchQuery(Constants.VERSION, "2.0"))
                    .must(QueryBuilders.matchQuery(Constants.TYPE, "ios_crash"))
                    .must(QueryBuilders.matchQuery("errorType", "EXC_CRASH (SIGABRT)-+[KSSystemInfo diskSpaceFree] (in 2.0) (KSSystemInfo.m:398)-*** Terminating app due to uncaught exception'NSInvalidArgumentException', reason:'-[__NSArrayI objectForKey:]: unrecognized selector sent to instance 0x15d29730'"))
                    //.must(QueryBuilders.matchQuery("errorType", "EXC_CRASH (SIGABRT)- [KSSystemInfo diskSpaceFree] (in 2.0) (KSSystemInfo.m:398)-*** Terminating app due to uncaught exception'NSInvalidArgumentException', reason:'-[__NSArrayI objectForKey:]: unrecognized selector sent to instance 0x15d29730'"))
                    .must(QueryBuilders.matchQuery(Constants.OPERATETYPE, 0))
                    .must(QueryBuilders.filteredQuery(QueryBuilders.rangeQuery(Constants.PROXY_TIMESTAMP).from(1464079802239L).to(1464079802239L), null));
            System.out.println(queryBuilder.toString());
            SearchResponse response = client.prepareSearch("bugly")
                    .setTypes("crash")
                    .setQuery(queryBuilder)
                    .addSort(Constants.PROXY_TIMESTAMP, SortOrder.DESC)
                    .setFrom(0)
                    .setSize(20)

                    .execute().actionGet();
            System.out.println(response);
        }finally {
            client.close();
        }
    }

    public static void filter(Client client, String appId, String version, String type, int operateState){
        try{
            BoolFilterBuilder filterBuilder = FilterBuilders.boolFilter();
            filterBuilder.must(FilterBuilders.termFilter(Constants.APPID, appId))
                    .must(FilterBuilders.rangeFilter(Constants.PROXY_TIMESTAMP).from(0).to(System.currentTimeMillis()));
            if (!"all".equals(version)) {
                filterBuilder.must(FilterBuilders.termFilter(Constants.VERSION, version));
            }
            if (!"all".equals(type)) {
                filterBuilder.must(FilterBuilders.termFilter(Constants.TYPE, type));
            }

            if (operateState == 0 || operateState == 1) {
                filterBuilder.must(FilterBuilders.termFilter(Constants.OPERATETYPE, operateState));
            }

            FilteredQueryBuilder filterQueryBuilder = QueryBuilders.filteredQuery(null, filterBuilder);

            AbstractAggregationBuilder aggBuilder = AggregationBuilders.terms("count").field("errorType")
                    .subAggregation(AggregationBuilders.terms("versionCount").field(Constants.VERSION)
                            .subAggregation(AggregationBuilders.terms("typeCount").field(Constants.TYPE)
                                    .subAggregation(AggregationBuilders.terms("operateCount").field(Constants.OPERATETYPE)
                                            .subAggregation(AggregationBuilders.cardinality("uvPerVersion").field(Constants.SUID))
                                            .subAggregation(AggregationBuilders.max("recent").field(Constants.PROXY_TIMESTAMP))
                                            .subAggregation(AggregationBuilders.min("latest").field(Constants.PROXY_TIMESTAMP)))));
            //queryBuilder = queryBuilder.must(QueryBuilders.matchAllQuery());

            SearchRequestBuilder searchRequestBuilder = client.prepareSearch("bugly").setTypes("crash")
                    .setQuery(filterQueryBuilder)
                    .addAggregation(aggBuilder);


            SearchResponse response = searchRequestBuilder.execute().actionGet();
            System.out.println(response);

            Map<String, Aggregation> aggMap = response.getAggregations().asMap();
            StringTerms count = (StringTerms) aggMap.get("count");
            Iterator<Terms.Bucket> countItr = count.getBuckets().iterator();
        }finally {
            client.close();
        }
    }

    public static void distributionMap(Client client){
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery(Constants.APPID, "com.example.chenpengpeng.tongjidemo2"))
                //.must(QueryBuilders.matchQuery(Constants.REPORT_TYPE, reportType))
                .must(QueryBuilders.rangeQuery(Constants.PROXY_TIMESTAMP).from(1463932800000L).to(1464019200000L));
//        if(!"all".equals(version)){
//            queryBuilder.must(QueryBuilders.matchQuery(Constants.VERSION, version));
//        }
        SearchRequestBuilder searchQueryBuilder = client.prepareSearch("bugly").setTypes("crash")
                .setQuery(queryBuilder)
                .addField("brand")
                .addAggregation(AggregationBuilders.terms("metric").field("@message.brand"));
        SearchResponse response = searchQueryBuilder.execute().actionGet();
        System.out.println(response);
    }

    public static void main(String[] args) throws Exception{

        String str = "cb:";
        String[] ary = str.split(":");
        Arrays.sort(ary);
        System.out.println(Arrays.toString(ary));

        //Client client = getTransportClient(9300, "es-cloud", "10.1.22.21");
        //Client client = getTransportClient(9300, "elasticsearch", "es.op.koudai.com");

        //addIndex(client);
        //get(client);
        //search(client);
        //analyze(client);
        //addMultIndex(client);
        //aggregation(client);
        //bulk(client);
        //searchAll(client);
        //addComplexIndex(client);
        //bulkFormal(client);
        //update(client);
        //histogram(client);
        //System.out.println(System.currentTimeMillis());
        //searchOne(client);
        //matchAll(client);
//        String errorType = "++==&&java.lang.NullPointerException-at \t        \rcom.cpp.tongji.TestCrash.NullPointerCrash(TestCrash.java)\n";
//        String replace = errorType.replaceAll("(&|=|\\+|\t|\n|\r|[\\s]{2,})", "").trim();
//        System.out.println(replace);

//        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("@message").startObject().field("state").value(1).endObject().endObject();
//        System.out.println(builder.string());

        //update(client);
        //filter(client, "weidian.GLStatisticalAnalysisDemo", "0.10", "all", -1);


    }
}
