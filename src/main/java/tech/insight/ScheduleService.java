package tech.insight;

import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * 演示如何实现一个可中断的定时任务服务
 *
 * @author gongxuanzhangmelt@gmail.com (原始作者)
 * @author Gemini (重构)
 */
public class ScheduleService {

    // 核心改造点1：使用一个 Map 统一管理任务
    // 它充当了“注册表”，也解决了 taskName 的判重问题
    private final ConcurrentHashMap<String, Job> activeJobs = new ConcurrentHashMap<>();

    private final Trigger trigger = new Trigger();

    public ExecutorService getExecutorService() {
        return executorService;
    }

    private final ExecutorService executorService = Executors.newFixedThreadPool(6);

    /**
     * 调度一个任务（固定延迟执行）
     * 类似 scheduleWithFixedDelay
     */
    void schedule(Runnable task, long delay, String taskName) {
        Job newJob = new Job();
        newJob.setTaskName(taskName);
        newJob.setTask(task);
        newJob.setDelay(delay);
        newJob.setStartTime(System.currentTimeMillis() + newJob.getDelay());

        // 核心改造点3：使用 putIfAbsent 保证原子性的“注册”
        // 如果 taskName 已存在，则 schedule 失败，直接返回
        if (activeJobs.putIfAbsent(taskName, newJob) != null) {
            System.out.println("任务 [" + taskName + "] 已经存在，调度失败。");
            return;
        }

        // 注册成功后，加入队列
        trigger.queue.offer(newJob);
        System.out.println("任务 [" + taskName + "] 调度成功。");
        trigger.wakeUp();
    }

    /**
     * 核心改造点4：实现可中断的任务
     */
    void cancelTask(String taskName) {
        // 1. 从“注册表”中移除，这是最关键的一步
        // 移除后，任务在执行完毕后，就不会再被重新调度了（见 Trigger 循环）
        Job jobToCancel = activeJobs.remove(taskName);

        if (jobToCancel == null) {
            System.out.println("任务 [" + taskName + "] 不存在或已被取消。");
            return;
        }

        // 2. 从等待队列中移除（如果它还在排队的话）
        // PriorityBlockingQueue.remove(Object) 是 O(N) 复杂度，但线程安全
        boolean removedFromQueue = trigger.queue.remove(jobToCancel);
        if (removedFromQueue) {
            System.out.println("任务 [" + taskName + "] 已从等待队列中移除。");
        }

        // 3. 中断正在运行的任务（如果它正在运行）
        Future<?> runningTask = jobToCancel.getRunningFuture();
        if (runningTask != null) {
            // true 表示：如果任务正在运行，则中断该线程
            // 这就需要你被调度的任务(task)自身能响应中断
            boolean cancelled = runningTask.cancel(true);
            if (cancelled) {
                System.out.println("任务 [" + taskName + "] 正在运行，已发送中断信号。");
            } else {
                System.out.println("任务 [" + taskName + "] 运行已结束，无法中断。");
            }
        } else {
            System.out.println("任务 [" + taskName + "] 当前未在运行。");
        }
    }

    //  等待合适的时间，把对应的任务扔到线程池中
    class Trigger {

        // 依然使用优先队列
        final PriorityBlockingQueue<Job> queue = new PriorityBlockingQueue<>();

        final Thread thread = new Thread(() -> {
            while (true) {
                try {
                    // 从队列中获取最近的任务，如果队列为空，poll()会阻塞
                    // 我们改用 peek() + park() 组合，避免阻塞 poll() 导致 cancel 时的 remove() 操作异常
                    Job latelyJob;
                    while ((latelyJob = queue.peek()) == null) {
                        System.out.println("队列为空，Trigger 线程休眠...");
                        LockSupport.park();
                    }

                    long now = System.currentTimeMillis();
                    long startTime = latelyJob.getStartTime();

                    if (startTime <= now) {
                        // 时间到了，执行任务
                        Job jobToRun = queue.poll(); // 弹出

                        // 核心改造点5：检查任务是否在 poll() 之前被取消了
                        if (!activeJobs.containsKey(jobToRun.getTaskName())) {
                            System.out.println("任务 [" + jobToRun.getTaskName() + "] 在执行前被取消，跳过。");
                            continue;
                        }

                        System.out.println("任务 [" + jobToRun.getTaskName() + "] 开始执行...");
                        // 使用 submit 而不是 execute，并保存 Future
                        Future<?> future = executorService.submit(jobToRun.getTask());
                        jobToRun.setRunningFuture(future);

                        // 任务执行完后，（如果它没被取消）重新计算下一次时间并放回队列
                        future.get(); // 等待任务执行完毕（这是 fixedDelay 的实现方式）

                        // 任务跑完后，清空 future
                        jobToRun.setRunningFuture(null);

                        // 核心改造点6：任务跑完后，检查是否在运行期间被取消了
                        // 只有任务还在"注册表"里，才进行下一次调度
                        Job currentJobInMap = activeJobs.get(jobToRun.getTaskName());
                        if (currentJobInMap == jobToRun) {
                            // 没被取消，计算下一次时间并重新入队
                            jobToRun.setStartTime(now + jobToRun.getDelay());
                            queue.offer(jobToRun);
                            System.out.println("任务 [" + jobToRun.getTaskName() + "] 执行完毕，重新调度。");
                        } else {
                            // 运行期间被 cancelTask 了
                            System.out.println("任务 [" + jobToRun.getTaskName() + "] 运行完毕，但已被取消，不再调度。");
                        }

                    } else {
                        // 时间没到，休眠到指定时间点
                        LockSupport.parkUntil(startTime);
                    }
                } catch (InterruptedException e) {
                    System.out.println("Trigger 线程被中断，退出...");
                    break; // 退出循环
                } catch (ExecutionException e) {
                    System.err.println("任务执行时发生异常: " + e.getCause());
                    // 异常处理逻辑，比如是否重试等
                }
            }
        });

        {
            thread.setDaemon(true); // 设置为守护线程，主程序退出时它也退出
            thread.setName("ScheduleTriggerThread");
            thread.start();
            System.out.println("触发器启动了!");
        }

        void wakeUp() {
            LockSupport.unpark(thread);
        }
    }


    // === 测试一下 ===

}