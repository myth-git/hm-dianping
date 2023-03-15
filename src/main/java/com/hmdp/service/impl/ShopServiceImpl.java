package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.DemoClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 解决缓存穿透
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        // 7.返回
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }

    @Autowired
    private DemoClient demoClient;

    @Override
    public Result queryShopTestById(Long id) {
//        String key = "Test_Shop:" + id;
//        //1.从redis根据id查数据
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2.判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //3.若存在直接返回数据
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
//        }
//        if (shopJson != null) {//isNotBlank 判断""和null时返回的false，所以不会走上面代码也不能走上面的代码，因为缓存的空对象是""，所以不等于null
//            return Result.fail("该店铺不存在相关信息");
//        }
//        //4.从数据库中根据id查数据
//        Shop shop = this.getById(id);
//        //5.若不存在，返回错误信息
//        if (shop == null) {
//            //5.1 写入空值缓存中
//            stringRedisTemplate.opsForValue().set(key, "",2L, TimeUnit.MINUTES);
//            return Result.fail("店铺不存在！！");
//        }
//
//        //6.将数据写入redis
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
        //缓存穿透
//        Shop shop = this.queryWithPassThrough(id);
//        Shop shop = this.queryWithMutex(id);
//        Shop shop = this.queryWithLoginExpire(id);
        Shop shop = demoClient.queryWithPassThrough("Test_Shop:", id, Shop.class, id2 -> this.getById(id2), 30L, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("数据为空");
        }
        //7.返回数据
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result updateTestShop(Shop shop) {

        if (shop.getId() == null) {
            return Result.fail("店铺id不能为空！！");
        }

        //1、更新数据库
        this.update(shop);

        //2、删除缓存
        stringRedisTemplate.delete("Test_Shop:" + shop.getId());
        return Result.ok();
    }

    //定义互斥锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //BooleanUtil.isTrue flag为true时，返回true，false或者null时返回false，避免空指针
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
    //缓存穿透
    public Shop queryWithPassThrough(Long id) {
        String key = "Test_Shop:" + id;
        //1.从redis根据id查数据
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.若存在直接返回数据
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if (shopJson != null) {//isNotBlank 判断""和null时返回的false，所以不会走上面代码也不能走上面的代码，因为缓存的空对象是""，所以不等于null
            return null;
        }
        //4.从数据库中根据id查数据
        Shop shop = this.getById(id);
        //5.若不存在，返回错误信息
        if (shop == null) {
            //5.1 写入空值缓存中
            stringRedisTemplate.opsForValue().set(key, "",2L, TimeUnit.MINUTES);
            return null;
        }

        //6.将数据写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);

        //7.返回数据
        return shop;
    }

    //缓存击穿--互斥锁
    public Shop queryWithMutex(Long id) {
        String key = "Test_Shop:" + id;
        //1.从redis根据id查数据
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.若存在直接返回数据
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if (shopJson != null) {//isNotBlank 判断""和null时返回的false，所以不会走上面代码也不能走上面的代码，因为缓存的空对象是""，所以不等于null
            return null;
        }

        //4.未命中，实现缓存重建
        //4.1获取互斥锁
        String lockKey = "Test_Shop_Lock:" + id;
        Shop shop = null;
        try {
            boolean tryLock = tryLock(lockKey);
            //4.2判断是否获取成功
            if (!tryLock) {
                //4.3失败，休眠并重试
                Thread.sleep(50);
                queryWithMutex(id);
            }
            //4.4成功查询数据库id
            shop = this.getById(id);
            //模拟重建时间
            Thread.sleep(200);
            //5.若不存在，返回错误信息
            if (shop == null) {
                //5.1 写入空值缓存中
                stringRedisTemplate.opsForValue().set(key, "",2L, TimeUnit.MINUTES);
                return null;
            }

            //6.将数据写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放锁
            unlock(lockKey);
        }
        //8.返回数据
        return shop;
    }

    //预热数据
    public void saveShopRedis(Long id, Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = this.getById(id);
        //模拟延迟,因为是本机测试，基本上一执行就介绍，所以需要加个延迟
        Thread.sleep(200);
        //2、封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3、写入redis JSONUtil.toJsonStr对象进行序列化
        stringRedisTemplate.opsForValue().set("Test_Shop:" + id, JSONUtil.toJsonStr(redisData));
    }

    //自定义一个线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //缓存击穿--逻辑过期
    public Shop queryWithLoginExpire(Long id) {
        String key = "Test_Shop:" + id;
        //1.从redis根据id查数据
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //3.若不存在直接返回数据
            return null;
        }
        //4.命中，需要先吧json反系列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期，返回商铺信息
            return shop;
        }

        //5.2过期，需要缓存重建
        //6.缓存重建
        //6.1尝试获取互斥锁
        String lockKey = "Test_Shop_Lock:" + id;
        boolean tryLock = this.tryLock(lockKey);
        //6.2判断是否获取锁成功
        if (tryLock) {
            //6.3成功，开启独立线程，从数据库中根据id查数据
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //重建缓存
                try {
                    this.saveShopRedis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });

        }

        //7.返回数据
        return shop;
    }
}
