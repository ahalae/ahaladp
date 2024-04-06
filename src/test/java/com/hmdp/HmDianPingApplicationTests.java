package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.service.IShopService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private IUserService UserServiceImpl;
    @Resource
    RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    IShopService shopService;
    @Resource
    CacheClient cacheClient;



    @Test
    public void loginAll() throws FileNotFoundException {
        List<User> users = UserServiceImpl.list();

        FileOutputStream outputStream = new FileOutputStream("C:/Users/AHaLa/Desktop/02-实战篇/资料/tokens.txt");
        // 根据输出流，创建一个PrintWriter
        PrintWriter printWriter = new PrintWriter(outputStream);
        for(User user:users){
            String token = UUID.randomUUID().toString(true);
            //user->hash
            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
            Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(), CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
            //存储
            String tokenKey=LOGIN_USER_KEY+token;
            stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
            //设置有效期
            stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL, TimeUnit.SECONDS);


            // 写入文件
            printWriter.println(token);

        }
        printWriter.flush();
        // 关闭流
        printWriter.close();


    }
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
    @Test
    void loadShopData(){
        //查询店铺信息
        List<Shop> list = shopService.list();
        //按typeid分组
        Map<Long, List<Shop>> map = list.stream()
                .collect(Collectors.groupingBy(Shop::getTypeId));
        //分批写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //获取类型id
            Long typeId = entry.getKey();
            String key=SHOP_GEO_KEY+typeId;
            //获取集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = value.stream()
                    .map(shop -> new RedisGeoCommands.GeoLocation<>(shop.getId().toString(), new Point(shop.getX(), shop.getY())))
                    .collect(Collectors.toList());
            //写入redis
//            for (Shop shop : value) {
//                stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
//            }
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Test
    public void testSendMessage2SimpleQueue(){
        String queueName="simple.queue";
        String message="hello world";
        rabbitTemplate.convertAndSend(queueName,message);
    }

}
