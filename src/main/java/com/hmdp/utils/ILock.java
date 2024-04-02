package com.hmdp.utils;

public interface ILock {
    /***
     * @param timeoutSec 锁持有的超时时间
     * @return true代表获取锁成功，false代表获取锁失败
     */
    boolean tryLock(long timeoutSec);
    /***
     * 释放锁
     */
    void unlock();
}
