package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @Description:
 * @Author: xzw
 * @Date: 2023/3/5 14:13
 */
@Component
@Slf4j
public class DemoClient {

    private  StringRedisTemplate stringRedisTemplate;

    public DemoClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit timeUnit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    public void setWithLogicTimeExpire(String key, Object value, Long time, TimeUnit timeUnit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透
    public <R,ID> R queryWithPassThrough(String prefix, ID id, Class<R> type, Function<ID,R> rollBack, Long time, TimeUnit timeUnit) {
//        String key = "Test_Shop:" + id;
        String key = prefix + id;
        //1.从redis根据id查数据
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3.若存在直接返回数据
            R r = JSONUtil.toBean(json, type);
            return r;
        }
        if (json != null) {//isNotBlank 判断""和null时返回的false，所以不会走上面代码也不能走上面的代码，因为缓存的空对象是""，所以不等于null
            return null;
        }
        //4.从数据库中根据id查数据
        R r = rollBack.apply(id);
        //5.若不存在，返回错误信息
        if (r == null) {
            //5.1 写入空值缓存中
//            stringRedisTemplate.opsForValue().set(key, "",2L, TimeUnit.MINUTES);
            this.set(key,"", 2L, TimeUnit.MINUTES);
            return null;
        }

        //6.将数据写入redis
        this.set(key,r, time, timeUnit);

        //7.返回数据
        return r;
    }


    //缓存击穿--互斥锁
    public <R, ID> R queryWithMutex(String prefix, String prefixLock, ID id, Class<R> type, Function<ID,R> rollBack
    , Long time, TimeUnit timeUnit) {
//        String key = "Test_Shop:" + id;
        String key = prefix + id;
        //1.从redis根据id查数据
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3.若存在直接返回数据
            R r = JSONUtil.toBean(json, type);
            return r;
        }
        if (json != null) {//isNotBlank 判断""和null时返回的false，所以不会走上面代码也不能走上面的代码，因为缓存的空对象是""，所以不等于null
            return null;
        }

        //4.未命中，实现缓存重建
        //4.1获取互斥锁
        String lockKey = prefixLock + id;
        R r = null;
        try {
            boolean tryLock = tryLock(lockKey);
            //4.2判断是否获取成功
            if (!tryLock) {
                //4.3失败，休眠并重试
                Thread.sleep(50);
                queryWithMutex(prefix,prefixLock, id, type, rollBack, time, timeUnit);
            }
            //4.4成功查询数据库id
//            shop = this.getById(id);
             r = rollBack.apply(id);
            //模拟重建时间
            Thread.sleep(200);
            //5.若不存在，返回错误信息
            if (r == null) {
                //5.1 写入空值缓存中
                stringRedisTemplate.opsForValue().set(key, "",2L, TimeUnit.MINUTES);
                return null;
            }

            //6.将数据写入redis
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
            this.set(key, r,time, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放锁
            unlock(lockKey);
        }
        //8.返回数据
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
