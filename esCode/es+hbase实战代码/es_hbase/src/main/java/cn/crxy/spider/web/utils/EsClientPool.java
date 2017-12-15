package com.koudai.common.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 此对象很重,需要保证单例
 * Created by zhangzhang on 16/8/29.
 */
public class EsClientPool {

    private static volatile EsClientPool instance;

    private static final Logger LOGGER = LogManager.getLogger(EsClientPool.class);

    //连接队列
    private ConcurrentLinkedQueue<Client> clientPool;
    //队列大小
    private int size;
    //集群名称
    private String clusterName;
    //集群ip
    private String[] ips;
    //集群端口
    private int port;

    private static AtomicLong fetchSucNum = new AtomicLong(0);

    private static AtomicLong fetchFailNum = new AtomicLong(0);


    private static AtomicLong returnSourceNum = new AtomicLong(0);

    //连接池中已有的连接
    private static AtomicInteger currentSize = new AtomicInteger(0);

    private EsClientPool(int size, String clusterName, String[] ips, int port){
        this.clusterName = clusterName;
        this.ips = ips;
        this.port = port;
        clientPool = new ConcurrentLinkedQueue<>();
        this.size = size;
    }

    public Client borrowClient(){
        LOGGER.info("fetch {} clients successful, {} clients failed", fetchSucNum.get(), fetchFailNum.get());
        if(currentSize.incrementAndGet() <= size){
            LOGGER.info("连接池为空,创建新连接,目前连接总数为{}", currentSize.get());
            Client client = EsUtils.getClient(port, clusterName, ips);
            fetchSucNum.incrementAndGet();
            return client;
        }
        synchronized (clientPool) {
            if (!clientPool.isEmpty()) {
                LOGGER.info("从队列中获取连接成功");
                fetchSucNum.incrementAndGet();
                return clientPool.poll();
            } else {
                try {
                    LOGGER.info("线程{}等待...", Thread.currentThread().getName());
                    clientPool.wait();
                    LOGGER.info("线程{}释放...", Thread.currentThread().getName());
                    fetchSucNum.incrementAndGet();
                    return clientPool.poll();
                } catch (InterruptedException e) {
                    LOGGER.error("获取es连接失败", e);
                    fetchFailNum.incrementAndGet();
                    return null;
                }
            }
        }

    }

    public void close(Client client){
        if(client != null){
            synchronized (clientPool) {
                returnSourceNum.incrementAndGet();
                clientPool.offer(client);
                clientPool.notify();
            }

        }
        LOGGER.info("return {} client back pool successful", returnSourceNum.get());
    }

    public void destroyPool(){
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                Client client = null;
                while((client = clientPool.poll()) != null){
                    client.close();
                    LOGGER.info("关闭连接池...");
                }
            }
        }));
    }


    public static void main(String[] args){
        final EsClientPool instance = new EsClientPool(10, "elasticsearch", new String[]{"es.op.koudai.com"}, 9300);
        final AtomicInteger i = new AtomicInteger(0);
        while(i.incrementAndGet() < 30){
            try {
                Thread.currentThread().sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            new Thread(new Runnable() {
                @Override
                public void run() {
                    int n = i.get();
                    Client client = instance.borrowClient();
                    if(client == null){
                        System.out.println("线程" + n + "获取连接失败");
                    }else{
                        System.out.println("线程" + n + "获取连接成功");

                        try {
                            Thread.currentThread().sleep(1000);
                            instance.close(client);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
        try {
            Thread.currentThread().sleep(1000 * 50);
            instance.destroyPool();
        } catch (InterruptedException e) {
            LOGGER.error(e);
        }

    }

    public static EsClientPool getInstance(String clusterName, int size, String[] ips, int port){
        if(instance == null){
            synchronized(EsClientPool.class){
                if(instance == null){
                    LOGGER.info("es client pool initialize starting ....");
                    instance = new EsClientPool(size, clusterName, ips, port);
                    LOGGER.info("es client pool initialize ending ....");
                }
            }
        }
        return instance;
    }

}
