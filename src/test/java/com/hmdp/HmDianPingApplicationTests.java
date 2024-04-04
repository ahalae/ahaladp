package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private IUserService UserServiceImpl;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
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

}
