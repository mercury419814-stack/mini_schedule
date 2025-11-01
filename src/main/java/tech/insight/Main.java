package tech.insight;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class Main {
    public static void main(String[] args) throws InterruptedException {
        ScheduleService service = new ScheduleService();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss SSS");

        // 一个可中断的任务
        Runnable interruptibleTask = () -> {
            try {
                System.out.println(LocalDateTime.now().format(dateTimeFormatter) +"--> (Task-A) 开始执行，预计 5 秒...");
                // 模拟耗时，且能响应中断
                Thread.sleep(5000);
                System.out.println(LocalDateTime.now().format(dateTimeFormatter) +"--> (Task-A) 执行完毕。");
            } catch (InterruptedException e) {
                System.out.println(LocalDateTime.now().format(dateTimeFormatter) +"--> (Task-A) 啊！我被中断了！正在清理...");
                // 恢复中断标志位，是个好习惯
                Thread.currentThread().interrupt();
            }
        };

        // 一个执行很快的任务
        Runnable fastTask = () -> {
            System.out.println(LocalDateTime.now().format(dateTimeFormatter) +"-----> (Task-B) 我很快，1秒完事。");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
        };

        // 调度A，每10秒一次（从现在开始10秒后）
        service.schedule(interruptibleTask, 10_000, "Task-A");
        // 调度B，每3秒一次（从现在开始3秒后）
        service.schedule(fastTask, 3_000, "Task-B");

        // 让子弹飞一会儿
        Thread.sleep(7000);
        // 此时 Task-B 应该已经跑了 2 次
        // Task-A 应该还在队列里等着（10秒才到期）

        System.out.println(LocalDateTime.now().format(dateTimeFormatter) +"\n======= 准备取消 Task-A (它还没开始) =======");
        service.cancelTask("Task-A");
        System.out.println("=========================================\n");

        Thread.sleep(4000);
        // 此时 Task-A 的10秒启动时间到了，但因为它被取消了，所以不会执行
        // Task-B 应该还在欢快地跑

        System.out.println(LocalDateTime.now().format(dateTimeFormatter) +"\n======= 准备调度 Task-A (耗时5秒) =======");
        service.schedule(interruptibleTask, 1000, "Task-A"); // 1秒后启动

        Thread.sleep(2000); // 等 Task-A 启动

        System.out.println(LocalDateTime.now().format(dateTimeFormatter) +"\n======= 准备取消 Task-A (它正在运行) =======");
        service.cancelTask("Task-A");
        System.out.println("=========================================\n");

        Thread.sleep(10000);
        System.out.println(LocalDateTime.now().format(dateTimeFormatter) +"\n======= 10秒后，Task-A 应该没有再次执行 =======");

        // 关闭（因为 Trigger 是守护线程，这里只是为了关闭 Executor）
        service.getExecutorService().shutdownNow();
    }
}
