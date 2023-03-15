package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: xzw
 * @Date: 2023/3/11 14:38
 */
public class SimpleRedisLockTest implements ILock {

    private String name;
    private StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLockTest(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final  String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    //提交加载lua脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标识
//        long threadId = Thread.currentThread().getId();
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁 key表示锁名称 value表示线程名称
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        //boolean和Boolean 会产生自动拆箱问题，所以加一层，true返回true false或者null 返回null
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        // 调用lua脚本
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }

//    @Override
//    public void unlock() {
//        //获取线程标识
//        String threadId = ID_PREFIX + Thread.currentThread().getId();
//        //获取锁中的标识
//        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//        //判断标识是否一致
//        if (threadId.equals(id)) {
//            //释放锁
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
//    }
}
