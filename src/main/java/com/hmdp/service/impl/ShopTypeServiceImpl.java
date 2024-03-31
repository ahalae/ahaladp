package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        //从redis查询缓存
        String key="typelist";
        List<String> typeStringList = stringRedisTemplate.opsForList().range(key, 0, stringRedisTemplate.opsForList().size(key));


        if (typeStringList!=null&&typeStringList.size()>0) {
            //存在则返回
            List<ShopType> shopTypeList = typeStringList.stream().map(o -> JSONUtil.toBean(o, ShopType.class)).collect(Collectors.toList());
            return Result.ok(shopTypeList);
        }
        //不存在，读数据库
        List<ShopType> shopTypeList = list();
        //不存在，返回错误
        if(shopTypeList==null||shopTypeList.size()==0) {
            return Result.fail("店铺不存在！");
        }
        //存在写入redis
        stringRedisTemplate.opsForList().rightPushAll(key,shopTypeList.stream().map(o->JSONUtil.toJsonStr(o)).collect(Collectors.toList()));

        //返回
        return Result.ok(shopTypeList);
    }
}
