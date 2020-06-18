 /*
  * Package tk.tak
  * FileName: ThreadTool
  * Author:   Tak
  * Date:     18/11/26 20:33
  */
 package tk.tak.hive;

 import java.util.List;
 import java.util.Queue;
 import java.util.concurrent.ArrayBlockingQueue;
 import java.util.concurrent.LinkedBlockingQueue;
 import java.util.concurrent.ThreadFactory;
 import java.util.concurrent.ThreadPoolExecutor;
 import java.util.concurrent.TimeUnit;
 import java.util.concurrent.atomic.AtomicInteger;
 import java.util.concurrent.locks.Condition;
 import java.util.concurrent.locks.ReentrantLock;

 /**
  * @author Tak
  */
 public class ThreadTool {
	 private ThreadPoolExecutor threadPoolExecutor;

	 private static final ThreadTool ONE = new ThreadTool();

	 private volatile boolean running = false;

	 private Queue<Runnable> asynTask = new ArrayBlockingQueue<>(10000);
	 private final ReentrantLock lock = new ReentrantLock();
	 private Condition condition = lock.newCondition();


	 public static ThreadTool getONE() {
		 return ONE;
	 }

	 public void addTask(List<Runnable> runnableList) {
		 asynTask.addAll(runnableList);
	 }

	 public boolean addTask(Runnable r) {
		 boolean ret = asynTask.offer(r);
		 if (!ret) {
			 System.out.println("full");
		 }
		 return ret;
	 }


	 private boolean checkIfRunning() {
		 boolean state = false;
		 lock.lock();
		 try {
			 state = running;
		 } finally {
			 lock.unlock();
		 }
		 return state;
	 }

	 public void start() throws InterruptedException {
		 if (!running) {
			 lock.lock();
			 try {
				 startAsyncThreadPool();
			 } catch (Exception e) {
				 running = false;
				 throw new InterruptedException();
			 } finally {
				 running = true;
				 lock.unlock();
			 }
		 }
	 }

	 private void startAsyncThreadPool() {
		 threadPoolExecutor = new ThreadPoolExecutor(12, 12, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(10000), new DefaultThreadFactory());
		 for (int i = 0, core = Runtime.getRuntime().availableProcessors(); i < core; i++) {
			 threadPoolExecutor.execute(new BaseRunnable());
		 }
	 }

	 private static class DefaultThreadFactory implements ThreadFactory {
		 private final ThreadGroup group;
		 private final AtomicInteger threadNumber = new AtomicInteger(1);
		 private final String namePrefix;

		 DefaultThreadFactory() {
			 SecurityManager s = System.getSecurityManager();
			 group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
			 namePrefix = "async-deal-";
		 }

		 public Thread newThread(Runnable r) {
			 Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
			 if (t.isDaemon())
				 t.setDaemon(false);
			 if (t.getPriority() != Thread.NORM_PRIORITY)
				 t.setPriority(Thread.NORM_PRIORITY);
			 return t;
		 }
	 }

	 class BaseRunnable implements Runnable {

		 @Override
		 public void run() {
			 for (; ThreadTool.this.checkIfRunning() && !threadPoolExecutor.isShutdown(); ) {
				 if (asynTask.size() == 0) {
					 continue;
				 }
				 if (threadPoolExecutor.isShutdown()) {
					 break;
				 }
				 Runnable r;
				 for (int i = 0, size = asynTask.size(); i < size && (r = asynTask.peek()) != null; i++) {
					 threadPoolExecutor.execute(r);
					 asynTask.poll();
				 }
			 }

		 }
	 }


 }
