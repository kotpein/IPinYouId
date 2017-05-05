package com.epam.hadoop;

import java.util.concurrent.ConcurrentHashMap;

public class IPinYouCounter implements Runnable {

	private final ConcurrentHashMap<String, Integer> map;

	
	public IPinYouCounter(ConcurrentHashMap<String, Integer> map, String line) {
		this.map = map;
		this.line = line;
	}

	private String line;

	@Override
	public void run() {
		String[] fields = line.split("\\s");
		String ipnId = fields[3];
		evaluate(ipnId);
	}

	private void evaluate(String ipnId) {
		synchronized (ipnId) {
			Integer amount = map.get(ipnId);
			amount = amount == null ? 1 : amount + 1;
			map.put(ipnId, amount);
		}
	}
}
