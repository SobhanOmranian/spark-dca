package org.apache.spark.dca;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class MyUtils {
	private final static Logger log = Logger.getLogger("MyUtils");
	
	public static Long timeTakenInRead = 0L;

	public static List<String> readFileInListFromTo(String fileName, int from) {
		List<String> lines = readFileInList(fileName);
		int to = lines.size();
		log.debug(String.format("Reading from lines (%s) to (%s)", from, to));
		if (to < from) {

			log.debug(String.format("For some reason, to is less than from:  (%s) to (%s)", from, to));
			return null;
		}

		
		return lines.subList(from, to);
	}
	public static List<String> readFileInList(String fileName) {
//		return Collections.emptyList();
		Long startTime = System.currentTimeMillis();
		
		List<String> lines = Collections.emptyList();
		
		
		try {
			lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
		}

		catch (IOException e) {
			e.printStackTrace();
		}
		
		Long elapsed = System.currentTimeMillis() - startTime;
		log.debug(String.format("[READ TIME]: time to read file [%s] into a list: %s ms", fileName, elapsed));
		timeTakenInRead += elapsed;
		log.debug(String.format("[READ TIME]: total time in read: %s", timeTakenInRead));
		return lines;
	}
	
	public static float getReadThroughputFromLine(String line) {
		float result = 0;

		result = Float.valueOf(line.split(",")[13]);
		result += Float.valueOf(line.split(",")[14]);

		return result;
	}
	public static float getTimeFromLine(String line) {
		float result = 0;
//		String pattern = "<[\\d.+]*>";
		String pattern = "<[+-]?([0-9]*[.])?[0-9]+>";
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(line);
		if (m.find()) {
			String value = m.group(0);
			value = value.substring(1, value.length() - 1);
			result = Float.parseFloat(value);
		}

		return result;
	}
	public static int calculateAverage(Queue<Long> marks) {
		long sum = 0;
		for (Long mark : marks) {
			sum += mark;
		}
		log.debug(String.format("Sum: %s, size: %s", sum, marks.size()));
		return marks.isEmpty() ? 0 : (int) sum / marks.size();
	}
	
	public static String getHostName() {
		String hostname = "default";
		InetAddress addr;
		try {
			addr = InetAddress.getLocalHost();
			hostname = addr.getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		return hostname;
	}
}
