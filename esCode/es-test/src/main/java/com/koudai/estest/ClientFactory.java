package com.koudai.estest;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by liupinghe.
 */
public class ClientFactory {

    public static TransportClient getClient() throws UnknownHostException {

        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        return client;

    }


    private static TransportClient transPort = null;
    private String esClusterName;//集群名

    private String esServerIps;//集群服务IP集合

    private Integer esServerPort;//ES集群端口

    public static TransportClient getTransPort() {
        return transPort;
    }

    public static void setTransPort(TransportClient transPort) {
        ClientFactory.transPort = transPort;
    }

    public String getEsClusterName() {
        return esClusterName;
    }

    public void setEsClusterName(String esClusterName) {
        this.esClusterName = esClusterName;
    }

    public String getEsServerIps() {
        return esServerIps;
    }

    public void setEsServerIps(String esServerIps) {
        this.esServerIps = esServerIps;
    }

    public Integer getEsServerPort() {
        return esServerPort;
    }

    public void setEsServerPort(Integer esServerPort) {
        this.esServerPort = esServerPort;
    }


    /**
     * ES TransPortClient 客户端连接<br>
     * 在elasticsearch平台中,可以执行创建索引,获取索引,删除索引,搜索索引等操作
     *
     * @return
     */
    public TransportClient getTransPortClient() {
        try {
            if (transPort == null) {

                if (esServerIps == null || "".equals(esServerIps.trim())) {
                    return null;
                }

                Settings settings = Settings.builder()
                        .put("cluster.name", esClusterName)// 集群名
                        //In order to enable sniffing, set client.transport.sniff to true:
                        .put("client.transport.sniff", true)
                        .put("client.transport.ignore_cluster_name",true)
                        .put("client.transport.ping_timeout",5, TimeUnit.SECONDS)
                        .put("client.transport.nodes_sampler_interval",5,TimeUnit.SECONDS)
                        // 自动把集群下的机器添加到列表中

                        .build();
                transPort = new PreBuiltTransportClient(settings);
                String esIps[] = esServerIps.split(",");
                for (String esIp : esIps) {//添加集群IP列表
                    TransportAddress transportAddress = new InetSocketTransportAddress(InetAddresses.forString(esIp), 9300);
                    transPort.addTransportAddresses(transportAddress);
                }
                List<DiscoveryNode> connectedNodes = transPort.connectedNodes();
                for (DiscoveryNode discoveryNode : connectedNodes) {
                    System.out.print(discoveryNode.getHostName());
                }
                return transPort;
            } else {
                return transPort;
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (transPort != null) {
                transPort.close();
            }
            return null;
        }
    }


    /**
     * 使用es的帮助类创建JSON
     */
    public static XContentBuilder createJson4() throws Exception {
        // 创建json对象, 其中一个创建json的方式
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying to out ElasticSearch")
                .endObject();
        return source;
    }


    public static void main(String[] args) {
        ClientFactory clientFactory = new ClientFactory();
        clientFactory.setEsClusterName("my-application");
        clientFactory.setEsServerIps("10.8.96.11");
        clientFactory.setEsServerPort(9300);
        TransportClient client = clientFactory.getTransPortClient();
        //搜索数据
        GetResponse response = client.prepareGet("blog", "article", "1").execute().actionGet();
        //输出结果
        System.out.println(response.getSourceAsString());
        //关闭client
        client.close();
    }
}
