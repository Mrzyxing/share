/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		Configuration fileSystemConf = new Configuration();
		fileSystemConf.setBoolean("presto.s3.connection.ssl.enabled", false);
		fileSystemConf.setString("presto.s3.access-key", "minioadmin");
		fileSystemConf.setString("presto.s3.secret-key", "minioadmin");
		fileSystemConf.setString("presto.s3.endpoint", "http://127.0.0.1:9000");
		final String dir = "s3p://test/";

		FileSystem.initialize(fileSystemConf);
		Path path = new Path(dir);
		System.out.println(path.getFileSystem().exists(path));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		env.getConfig().setGlobalJobParameters(ParameterTool.fromSystemProperties());

		Configuration parameters = new Configuration();
		TextInputFormat format = new TextInputFormat(new Path(dir));
		format.setNestedFileEnumeration(true);
		format.configure(parameters);

		DataStream<String> dataStream = env.readFile(format, dir);
		dataStream.print();

		env.execute("Fraud Detection");
	}
}
