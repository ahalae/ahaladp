package com.hmdp.canal;

import com.github.benmanes.caffeine.cache.Cache;
import com.hmdp.entity.Shop;
import com.hmdp.utils.CacheClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import top.javatool.canal.client.annotation.CanalTable;
import top.javatool.canal.client.handler.EntryHandler;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

@CanalTable("tb_shop")
@Component
public class ShopHandler implements EntryHandler<Shop> {

    @Autowired
    private CacheClient cacheClient;
    @Autowired
    private Cache<Long,Shop> shopCache;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public void insert(Shop shop) {
        //写到本地缓存
        shopCache.put(shop.getId(),shop);
        //写到redis
        cacheClient.set(CACHE_SHOP_KEY+shop.getId(),shop,CACHE_SHOP_TTL,TimeUnit.MINUTES);
    }

    @Override
    public void update(Shop before, Shop after) {
        //写到本地缓存
        shopCache.put(after.getId(),after);
        //写到redis
        cacheClient.set(CACHE_SHOP_KEY+after.getId(),after,CACHE_SHOP_TTL,TimeUnit.MINUTES);
    }

    @Override
    public void delete(Shop shop) {
        //删除本地缓存
        shopCache.invalidate(shop.getId());
        //删除redis
        stringRedisTemplate.delete(CACHE_SHOP_KEY+shop.getId());
    }
}
