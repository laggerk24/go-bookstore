import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// ---------- JOB INTERFACE ----------
interface Job {
    void run();
}

// ---------- SCHEDULED JOB CLASS ----------
class ScheduledJob implements Comparable<ScheduledJob> {
    private final String jobId;
    private final long scheduledTime; // Epoch milliseconds
    private final Job job;

    public ScheduledJob(long scheduledTime, Job job) {
        this.jobId = UUID.randomUUID().toString();
        this.scheduledTime = scheduledTime;
        this.job = job;
    }

    public String getJobId() {
        return jobId;
    }

    public long getScheduledTime() {
        return scheduledTime;
    }

    public Job getJob() {
        return job;
    }

    @Override
    public int compareTo(ScheduledJob other) {
        return Long.compare(this.scheduledTime, other.scheduledTime);
    }

    @Override
    public String toString() {
        return "Job[" + jobId + ", time=" + scheduledTime + "]";
    }
}

// ---------- JOB SCHEDULER SINGLETON ----------
public class JobScheduler {
    private static volatile JobScheduler instance;

    private final PriorityQueue<ScheduledJob> jobQueue;
    private final ExecutorService executorService;
    private volatile boolean running;

    private JobScheduler(int threadPoolSize) {
        this.jobQueue = new PriorityQueue<>();
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        this.running = true;
        startJobMonitor();
    }

    // Thread-safe Singleton instance
    public static JobScheduler getInstance(int threadPoolSize) {
        if (instance == null) {
            synchronized (JobScheduler.class) {
                if (instance == null) {
                    instance = new JobScheduler(threadPoolSize);
                }
            }
        }
        return instance;
    }

    // Schedule a new job
    public synchronized void scheduleJob(ScheduledJob job) {
        jobQueue.add(job);
        System.out.println("[SCHEDULER] Job scheduled: " + job);
        notify(); // Wake up monitor thread if sleeping
    }

    // Main job monitor loop
    private void startJobMonitor() {
        Thread monitorThread = new Thread(() -> {
            while (running) {
                try {
                    ScheduledJob nextJob = null;

                    synchronized (this) {
                        while (jobQueue.isEmpty() && running) {
                            wait(); // Wait until a new job is added
                        }
                        if (!running) break;

                        long now = System.currentTimeMillis();
                        nextJob = jobQueue.peek();

                        if (nextJob != null && nextJob.getScheduledTime() <= now) {
                            jobQueue.poll();
                        } else {
                            long waitTime = nextJob != null ? nextJob.getScheduledTime() - now : 1000;
                            wait(Math.max(waitTime, 100)); // Sleep until next job is due
                            continue;
                        }
                    }

                    // Execute job outside synchronized block
                    if (nextJob != null) {
                        ScheduledJob jobToRun = nextJob;
                        executorService.submit(() -> {
                            System.out.println("[EXECUTOR] Running job: " + jobToRun.getJobId());
                            jobToRun.getJob().run();
                        });
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println("[SCHEDULER] Job monitor stopped.");
        });

        monitorThread.setDaemon(true); // Allows JVM to exit if main thread ends
        monitorThread.start();
    }

    // Graceful shutdown
    public synchronized void shutdown() {
        running = false;
        notifyAll(); // Wake up monitor thread to exit
        executorService.shutdown();
        System.out.println("[SCHEDULER] Shutdown initiated.");
    }
}
