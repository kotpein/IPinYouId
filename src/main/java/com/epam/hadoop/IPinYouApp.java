package com.epam.hadoop;

public class IPinYouApp {
	private static final String ERROR_MESSAGE = "ERROR. Wrong usage. Please try the folowing keys\n"
			+ " -input \t\t input path to .bz2 file or directory with bzip2 compressed files (example -input /tmp/inputdir)\n"
			+ " -output \t\t output path (example -input /tmp/outputdir)\n"
			+ " -maxNumThreads \t number of threads for processing files (optional, by default runs as much threads as minimum of number of files and avaliable cpu threds)\n"
			+ "example:\n"
			+ "java -jar IPinYouCounter.jar -input /tmp/inputdir -output /tmp/outputdir -maxNumThreads 16";

	private static String inputPath;
	private static String outputPath;
	private static int maxNumThreads;

	private static void init(String[] args) {
		for (int i = 0; i < args.length;) {
			String key = args[i++];
			String value = args[i++];
			switch (key) {
			case "-input":
				inputPath = value;
				break;
			case "-output":
				outputPath = value;
				break;
			case "-maxNumThreads":
				maxNumThreads = Integer.valueOf(value);
				break;
			default:
				System.out.println(ERROR_MESSAGE);
				System.exit(0);
			}
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int length = args.length;
		if (length != 4 || length != 6) {
			System.out.println(ERROR_MESSAGE);
			System.exit(0);
		}
		init(args);

	}

}
