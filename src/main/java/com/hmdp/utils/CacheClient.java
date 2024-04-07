package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    public CacheClient(StringRedisTemplate stringRedisTemplate,RedissonClient redissonClient){
        this.stringRedisTemplate=stringRedisTemplate;
        this.redissonClient=redissonClient;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData=new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit){
        //从redis查询缓存
        String key= keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        if (StrUtil.isNotBlank(shopJson)) {
            //存在则返回
            R r = JSONUtil.toBean(shopJson, type);
            return r;
        }
        //判断是否为空
        if(shopJson!=null){
            return null;
        }
        //不存在，读数据库
        R r = dbFallback.apply(id);
        //不存在，返回错误
        if(r==null) {
            //空值写入reids
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //存在写入redis
        this.set(key,r,time,unit);
        //返回
        return r;
    }

    public <R,ID> R queryWithBloomFilter(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit){

        //获取布隆过滤器
        RBloomFilter<String> bloomFilter = redissonClient.getBloomFilter(BLOOM_FILTER_KEY + keyPrefix);
        //从redis查询缓存
        String key= keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        if (StrUtil.isNotBlank(shopJson)) {
            //存在则返回
            R r = JSONUtil.toBean(shopJson, type);
            return r;
        }
        //判断是否存在于布隆过滤器
        if(!bloomFilter.contains(key)){
            return null;
        }
        //不存在，读数据库
        R r = dbFallback.apply(id);
        //不存在，返回错误，重写布隆过滤器
        if(r==null) {
            //空值写入布隆过滤器
            bloomFilter.add(key);
            return null;
        }
        //存在写入redis
        this.set(key,r,time,unit);
        //返回
        return r;
    }

    public static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit){
        //从redis查询缓存
        String key=keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        if (StrUtil.isBlank(shopJson)) {
            //不存在则读入
            //判断是否为空
            if(shopJson!=null){
                return null;
            }
            //不存在，读数据库
            R r = dbFallback.apply(id);
            this.setWithLogicalExpire(key,r,time,unit);
            return r;
        }
        //命中，反序列化
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime=redisData.getExpireTime();
        //判断过期时间
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期直接返回
            return r;
        }
        //过期缓存重建

        //获取互斥锁
        String lockKey=LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        if(isLock){
            //获取成功创建线程重建
            if(!expireTime.isAfter(LocalDateTime.now())){
                CACHE_REBUILD_EXECUTOR.submit(()->{
                    try {
                        //查询数据库
                        R r1 = dbFallback.apply(id);
                        //写入redis
                        this.setWithLogicalExpire(key,r1,time,unit);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }finally {
                        unlock(lockKey);
                    }
                });
            }
        }

        //直接返回过期数据
        return r;
    }

    public Shop queryWithMutex(Long id){
        //从redis查询缓存
        String key=CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        if (StrUtil.isNotBlank(shopJson)) {
            //存在则返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断是否为空
        if(shopJson!=null){
            return null;
        }
        //实现缓存重建
        //获取互斥锁
        String lockKey=LOCK_SHOP_KEY+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //判断是否获取
            if(!isLock){
                //失败则休眠
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //双重查询
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                //存在则返回
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }
            //判断是否为空
            if(shopJson!=null){
                return null;
            }

            //成功查询数据库
            shop = getById(id);
            //模拟重建延时
//            Thread.sleep(200);
            //不存在，返回错误
            if(shop==null) {
                //空值写入reids 解决缓存穿透
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //存在写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //释放锁
            unlock(lockKey);
        }

        //返回
        return shop;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

}
