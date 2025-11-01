package tech.insight;

import java.util.concurrent.Future;

/**
 * @author gongxuanzhangmelt@gmail.com
 **/
public class Job implements Comparable<Job> {
    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    private String taskName;

    public Future<?> getRunningFuture() {
        return runningFuture;
    }

    public void setRunningFuture(Future<?> runningFuture) {
        this.runningFuture = runningFuture;
    }

    // 核心改造点2：用于追踪正在运行的任务，以便中断它
    private volatile Future<?> runningFuture;

    private Runnable task;

    private long startTime;
    
    private long delay;

    public long getDelay() {
        return delay;
    }

    public Job setDelay(long delay) {
        this.delay = delay;
        return this;
    }

    public Runnable getTask() {
        return task;
    }

    public Job setTask(Runnable task) {
        this.task = task;
        return this;
    }

    public long getStartTime() {
        return startTime;
    }

    public Job setStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    @Override
    public int compareTo(Job o) {
        return Long.compare(this.startTime, o.startTime);
    }

    // 重写 equals 和 hashCode 对于在 PriorityBlockingQueue 中 remove(Object) 很重要
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Job job = (Job) o;
        return taskName.equals(job.taskName);
    }

    @Override
    public int hashCode() {
        return taskName.hashCode();
    }
}
