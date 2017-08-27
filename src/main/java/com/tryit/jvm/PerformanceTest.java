package com.tryit.jvm;

import java.io.PrintWriter;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by agebriel on 7/13/17.
 */
public class PerformanceTest
{
	private static final long MEGABYTE = 1024L * 1024L;

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}

	public static void main(String[] args) {
		// I assume you will know how to create a object Person yourself...
		List<Person> list = new ArrayList<Person>();
		long start = System.currentTimeMillis();
		for (int i = 0; i <= 100000; i++) {
			list.add(new Person("Jim", "Knopf"));
		}
		long end = System.currentTimeMillis();

		System.out.println("Time taken: " + (end - start) + "ms");
		// Get the Java runtime
		Runtime runtime = Runtime.getRuntime();
		runtime.traceInstructions(true);
		runtime.traceMethodCalls(true);

		// Run the garbage collector
		runtime.gc();

		// Calculate the used memory
		long memory = runtime.totalMemory() - runtime.freeMemory();
		System.out.println("Used memory is bytes: " + memory);
		System.out.println("Used memory is megabytes: "
			+ bytesToMegabytes(memory));
		System.out.println("Available processors: " + runtime.availableProcessors());
		System.out.println("max memory: " + runtime.maxMemory()/MEGABYTE + "Mb");
	}

	public static void premain(final Instrumentation inst) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					PrintWriter out = new PrintWriter(System.err);

					ThreadMXBean tb = ManagementFactory.getThreadMXBean();
					out.printf("Current thread count: %d%n", tb.getThreadCount());
					out.printf("Peak thread count: %d%n", tb.getPeakThreadCount());

					List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
					for (MemoryPoolMXBean pool : pools) {
						MemoryUsage peak = pool.getPeakUsage();
						out.printf("Peak %s memory used: %,d%n", pool.getName(), peak.getUsed());
						out.printf("Peak %s memory reserved: %,d%n", pool.getName(), peak.getCommitted());
					}

					Class[] loaded = inst.getAllLoadedClasses();
					out.println("Loaded classes:");
					for (Class c : loaded)
						out.println(c.getName());
					out.close();
				} catch (Throwable t) {
					System.err.println("Exception in agent: " + t);
				}
			}
		});
	}

	static class Person{
		String fName, lName;
		public Person(String fName, String lName){
			this.fName = fName;
			this.lName = lName;
		}
	}
}
