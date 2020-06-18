/*
 * Package:  tk.tak.kafka.multithread
 * FileName: MultiThreadProducer
 * Author:   Tak
 * Date:     23/7/2019 下午3:17
 * email:    br7roy@gmail.com
 */
package tk.tak.kafka.multithread;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * reference https://blog.csdn.net/u010003835/article/details/83000537
 * @author Tak
 */
public class MultiThreadProducer {
	public static final MultiThreadProducer ONE = new MultiThreadProducer();
	private final ThreadPoolExecutor poolExecutor;

	public MultiThreadProducer() {
		poolExecutor = new ThreadPoolExecutor(2, 4, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(300));
		Runtime.getRuntime().addShutdownHook(new Thread(poolExecutor::shutdown));
	}

	public static MultiThreadProducer getONE() {
		return ONE;
	}

	static class MultiProduceWorker implements Runnable {

		@Override
		public void run() {

		}
	}

	public static void main(String[] args) {
		Properties p = new Properties();


	}
}
