/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.launcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command line interface for the Spark launcher. Used internally by Spark
 * scripts.
 */
class Main {

	/**
	 * Usage: Main [class] [class args]
	 * <p>
	 * This CLI works in two different modes:
	 * <ul>
	 * <li>"spark-submit": if <i>class</i> is "org.apache.spark.deploy.SparkSubmit",
	 * the {@link SparkLauncher} class is used to launch a Spark application.</li>
	 * <li>"spark-class": if another class is provided, an internal Spark class is
	 * run.</li>
	 * </ul>
	 *
	 * This class works in tandem with the "bin/spark-class" script on Unix-like
	 * systems, and "bin/spark-class2.cmd" batch script on Windows to execute the
	 * final command.
	 * <p>
	 * On Unix-like systems, the output is a list of command arguments, separated by
	 * the NULL character. On Windows, the output is a command line suitable for
	 * direct execution from the script.
	 */
	public static void main(String[] argsArray) throws Exception {
//		for (int i = 0; i < argsArray.length; i++)
//			System.err.println("Args: " + argsArray[i]);

		checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

		List<String> args = new ArrayList<>(Arrays.asList(argsArray));
		String className = args.remove(0);

		boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
		AbstractCommandBuilder builder;
		if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
			try {
				builder = new SparkSubmitCommandBuilder(args);
			} catch (IllegalArgumentException e) {
				printLaunchCommand = false;
				System.err.println("Error: " + e.getMessage());
				System.err.println();

				MainClassOptionParser parser = new MainClassOptionParser();
				try {
					parser.parse(args);
				} catch (Exception ignored) {
					// Ignore parsing exceptions.
				}

				List<String> help = new ArrayList<>();
				if (parser.className != null) {
					help.add(parser.CLASS);
					help.add(parser.className);
				}
				help.add(parser.USAGE_ERROR);
				builder = new SparkSubmitCommandBuilder(help);
			}
		} else {
			builder = new SparkClassCommandBuilder(className, args);
		}

		Map<String, String> env = new HashMap<>();
		List<String> cmd = builder.buildCommand(env);
		if (printLaunchCommand) {
			System.err.println("Spark Command: " + join(" ", cmd));
			System.err.println("========================================");
		}

		if (isWindows()) {
			System.out.println(prepareWindowsCommand(cmd, env));
		} else {
			// In bash, use NULL as the arg separator since it cannot be used in an
			// argument.
			List<String> bashCmd = prepareBashCommand(cmd, env);

			// System.err.println("[DEBUG] Adding perf stat to spark command...");
			// List<String> addedCommands = new ArrayList<String>();
			// addedCommands.add("/usr/bin/perf");
			// addedCommands.add("stat");
			// addedCommands.add("-o");
			// addedCommands.add("driver-perf.txt");
			// System.err.println("[DEBUG] Pinning driver's process to the socket [" + 0 +
			// "]");
			// addedCommands.add("numactl");
			// addedCommands.add("--membind=0");
			// addedCommands.add("--cpunodebind=0");

			// bashCmd.addAll(0, addedCommands);

			// bashCmd.add(0, "/usr/bin/perf");
			// bashCmd.add(1, "stat");
			// bashCmd.add(2, "-o");
			// bashCmd.add(3, "driver-perf.txt");
			// bashCmd.add(4, "numactl");
			// bashCmd.add(5, "--membind=0");
			// bashCmd.add(6, "--physcpubind=0");
			String finalCommand = "";
			System.err.println("[DEBUG] bash command = " + bashCmd.toString());
			if (bashCmd.contains("org.apache.spark.deploy.worker.Worker") && bashCmd.contains("--perf")) {

				int index = bashCmd.indexOf("--perf");
				System.err.println("[DEBUG] We found perf in arguments at position " + index);
				String perfCmd = bashCmd.get(index + 1);
				System.err.println("[DEBUG] Perf command = " + perfCmd);

				List<String> addedCommands = Arrays.asList(perfCmd.split("\\s+"));

				bashCmd.addAll(0, addedCommands);
			} else {
				System.err.println("[DEBUG] Not worker or no perf");
			}

//			Map<String, String> envi = System.getenv();
//			System.err.println();
//			System.err.println();
//			for (String envName : envi.keySet()) {
//				System.err.format("%s=%s%n", envName, envi.get(envName));
//			}
//			System.err.println();
//			System.err.println();
//			String test = System.getProperty("testProperty");
//			System.err.println("testProperty is " + test);

			if (bashCmd.contains("org.apache.spark.deploy.worker.Worker") && bashCmd.contains("--strace")) {
				// System.getProperty("strace") != null

				int index = bashCmd.indexOf("--strace");
				System.err.println("[DEBUG] We found strace in arguments at position " + index);
				String straceCmd = bashCmd.get(index + 1);

				// String straceCmd = System.getProperty("strace");

				System.err.println("[DEBUG] strace command = " + straceCmd);

				List<String> addedCommands = Arrays.asList(straceCmd.split("\\s+"));

				bashCmd.addAll(0, addedCommands);
			} else {
				System.err.println("[DEBUG] Not worker or no strace");
			}

			for (String c : bashCmd) {
				System.out.print(c);
				System.out.print('\0');
			}
		}
	}

	/**
	 * Prepare a command line for execution from a Windows batch script.
	 *
	 * The method quotes all arguments so that spaces are handled as expected.
	 * Quotes within arguments are "double quoted" (which is batch for escaping a
	 * quote). This page has more details about quoting and other batch script fun
	 * stuff: http://ss64.com/nt/syntax-esc.html
	 */
	private static String prepareWindowsCommand(List<String> cmd, Map<String, String> childEnv) {
		StringBuilder cmdline = new StringBuilder();
		for (Map.Entry<String, String> e : childEnv.entrySet()) {
			cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
			cmdline.append(" && ");
		}
		for (String arg : cmd) {
			cmdline.append(quoteForBatchScript(arg));
			cmdline.append(" ");
		}
		return cmdline.toString();
	}

	/**
	 * Prepare the command for execution from a bash script. The final command will
	 * have commands to set up any needed environment variables needed by the child
	 * process.
	 */
	private static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
		if (childEnv.isEmpty()) {
			return cmd;
		}

		List<String> newCmd = new ArrayList<>();
		newCmd.add("env");

		for (Map.Entry<String, String> e : childEnv.entrySet()) {
			newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
		}
		newCmd.addAll(cmd);
		return newCmd;
	}

	/**
	 * A parser used when command line parsing fails for spark-submit. It's used as
	 * a best-effort at trying to identify the class the user wanted to invoke,
	 * since that may require special usage strings (handled by
	 * SparkSubmitArguments).
	 */
	private static class MainClassOptionParser extends SparkSubmitOptionParser {

		String className;

		@Override
		protected boolean handle(String opt, String value) {
			if (CLASS.equals(opt)) {
				className = value;
			}
			return false;
		}

		@Override
		protected boolean handleUnknown(String opt) {
			return false;
		}

		@Override
		protected void handleExtraArgs(List<String> extra) {

		}

	}

}
