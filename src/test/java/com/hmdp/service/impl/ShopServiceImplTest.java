package com.hmdp.service.impl;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import sun.awt.geom.AreaOp;

import javax.annotation.Resource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ShopServiceImplTest {
    @Resource
    ShopServiceImpl shopService=new ShopServiceImpl();
    @Resource
    private CacheClient cacheClient;

    @Resource
    RedisIdWorker redisIdWorker;
    private ExecutorService es= Executors.newFixedThreadPool(50);
    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch=new CountDownLatch(300);

        Runnable task = ()->{
            for(int i=0;i<100;i++){
                long id=redisIdWorker.nextId("order");
                System.out.println("id = "+id);
            }
            latch.countDown();;
        };
        long begin=System.currentTimeMillis();
        for(int i=0;i<300;i++){
            es.submit(task);
        }
        latch.await();
        long end=System.currentTimeMillis();
        System.out.println("time = "+(end-begin));
    }    @Test
    void saveShop2Redis() throws InterruptedException {
        Shop shop=shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY+1L,shop,10L, TimeUnit.SECONDS);
    }
}