package com.esoft.dp.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 循环线程，直到外部发出退出命令
 * 
 * @author taoshi
 * 
 */
public abstract class LoopRunnable implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(LoopRunnable.class);

    // 运行标志
    private boolean run = true;

    // 当前线程
    private Thread thread;

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public Thread getThread() {
        return thread;
    }

    /**
     * 唤醒线程
     */
    public void wakeUp() {
        if (thread != null) thread.interrupt();
    }

    /**
     * 停止线程
     * 
     * @param wait
     *            如果为true等会等待线程结束，否则立刻返回
     */
    public void stop(boolean wait) {
        run = false;
        if (wait == true && thread != null) {
            for (int i = 0; i < 5 && thread.isAlive(); i++) {
                try {
                    thread.interrupt();
                    thread.join();
                    break;
                } catch (InterruptedException e) {
                    sleep(1000);
                }
            }
        }
    }

    /**
     * 基本流程 ：1. 准备; 2. 循环执行指定方法 ; 3. 清理
     */
    @Override
    public void run() {
        prepare();
        logger.warn("{} is started!", thread);
        while (run)
            this.procedure();
        logger.warn("{} is stopped!", thread);
        cleanup();
    }

    /**
     * 获取线程名字
     * 
     * @return 名字
     */
    public String getName() {
        return getClass().getSimpleName();
    }

    /**
     * 处理过程
     */
    protected abstract void procedure();

    /**
     * 准备，设置线程变量为当前线程
     */
    protected void prepare() {
        thread = Thread.currentThread();
    }

    /**
     * 清理，置空线程变量
     */
    protected void cleanup() {
        thread = null;
    }

    /**
     * 休眠
     * 
     * @param time
     *            休眠时间
     */
    protected void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }
}
