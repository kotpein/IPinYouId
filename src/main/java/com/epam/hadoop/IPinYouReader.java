package com.epam.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Reads lines from single file compressed with BZip2 format
 * Uses {@code BZip2CompressorInputStream} class for decompresing .bz2 files.
 * Note that using of {@code BZip2CompressorInputStream} is not thread safe.
 * 
 * @author Petro_Kotsiuba
 *
 */
public class IPinYouReader {
	private File inputFile;
	private BufferedReader bufferedReader;
	private BZip2CompressorInputStream compressorInputStream;
	private InputStream inputStream;
	
	public IPinYouReader(File inputFile) {
		super();
		this.inputFile = inputFile;
	}

	public void init() throws IOException {
		
//		FileSystem fs = FileSystem.get(new Configuration());
//		fs.listFiles(new Path(""), false).next();
		inputStream = new FileInputStream(inputFile);
		compressorInputStream = new BZip2CompressorInputStream(inputStream);
		bufferedReader = new BufferedReader(new InputStreamReader(compressorInputStream));
	}

	public String readLine() throws IOException {
		return bufferedReader.readLine();
	}

	public void close() throws IOException {
		bufferedReader.close();
	}
	
	
	
}
