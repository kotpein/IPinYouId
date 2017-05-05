package com.epam.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class IPinYouProcessor {

	private final ConcurrentHashMap<String, Integer> idCounterMap;

	/**
	 * Thread pool for parallel execution counters
	 */
	private final ExecutorService counterExecutor;

	/**
	 * Thread pool for parallel processing files
	 */
	private final ExecutorService fileProcessExecutor;

	private final String inputpath;
	private final boolean processFilesInParallel;

	/**
	 * 
	 * @param inputpath
	 *            path to input file or directory
	 * @param processFilesInParallel
	 *            if true than process files in parallel if inputpath is
	 *            directory and has more than one file inside
	 */
	public IPinYouProcessor(String inputpath, boolean processFilesInParralell) {
		idCounterMap = new ConcurrentHashMap<>();
		counterExecutor = Executors.newCachedThreadPool();
		this.inputpath = inputpath;
		this.processFilesInParallel = processFilesInParralell;
		if (processFilesInParralell)
			fileProcessExecutor = new ThreadPoolExecutor(2, Runtime.getRuntime().availableProcessors(), 0L,
					TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		else
			fileProcessExecutor = null;
	}

	/**
	 * 
	 * @param inputpath
	 *            path to input file or directory
	 * @param processFilesInParallel
	 *            if true than process files in parallel if inputpath is
	 *            directory and has more than one file inside
	 * @param maxNumThreads
	 *            maximum number of parallel processing files
	 */
	public IPinYouProcessor(String inputpath, boolean processFilesInParallel, int maxNumThreads) {
		idCounterMap = new ConcurrentHashMap<>();
		counterExecutor = Executors.newCachedThreadPool();
		this.inputpath = inputpath;
		this.processFilesInParallel = processFilesInParallel;
		if (processFilesInParallel && maxNumThreads > 1)
			fileProcessExecutor = Executors.newFixedThreadPool(maxNumThreads);
		else
			fileProcessExecutor = null;
	}

	/**
	 * 
	 * @throws IOException
	 *             - If an I/O error occurs
	 * @throws InterruptedException
	 *             - if interrupted while waiting
	 */
	public void process() throws IOException, InterruptedException {
		File file = new File(inputpath);
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			if (processFilesInParallel && files.length > 1) {
				parallelProcessFiles(files);
			} else {
				processFiles(files);
			}
		} else {
			processFile(file);
		}
		counterExecutor.shutdown();
	}

	public boolean waitBeforeComplition(long timeout, TimeUnit timeUnit) throws InterruptedException {
		boolean fileProcessSuccess = fileProcessExecutor.awaitTermination(timeout, timeUnit);
		boolean counterSuccess = counterExecutor.awaitTermination(timeout, timeUnit);
		return counterSuccess && fileProcessSuccess;
	}

	private void parallelProcessFiles(File[] files) throws InterruptedException {
		for (File file : files) {
			Runnable task = new Runnable() {
				@Override
				public void run() {
					try {
						processFile(file);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			};
			fileProcessExecutor.execute(task);
		}
		fileProcessExecutor.shutdown();
	}

	private void processFiles(File[] files) throws IOException {
		for (File file : files) {
			processFile(file);
		}
	}

	private void processFile(File file) throws IOException {
		IPinYouReader reader = new IPinYouReader(file);
		reader.init();
		String line = reader.readLine();
		while (line != null) {
			IPinYouCounter counter = new IPinYouCounter(idCounterMap, line);
			counterExecutor.execute(counter);
			line = reader.readLine();
		}
		reader.close();
	}

}
