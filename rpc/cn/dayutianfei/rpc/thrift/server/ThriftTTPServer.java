/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.dayutianfei.rpc.thrift.server;

import cn.dayutianfei.loadserver.lib.thrift.protocol.Status;
import cn.dayutianfei.loadserver.lib.thrift.protocol.ThriftFlumeEvent;
import cn.dayutianfei.loadserver.lib.thrift.protocol.ThriftSourceProtocol;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * TTPServer mean TThreadPoolServer
 * @author dayutianfei
 *
 */
public class ThriftTTPServer {

	public static final Logger LOG = LoggerFactory.getLogger(ThriftTTPServer.class);
	/**
	 * Config param for the maximum number of threads this source should use to
	 * handle incoming data.
	 */
	public static final String CONFIG_THREADS = "threads";
	/**
	 * Config param for the hostname to listen on.
	 */
	public static final String CONFIG_BIND = "bind";
	/**
	 * Config param for the port to listen on.
	 */
	public static final String CONFIG_PORT = "port";
	private Integer port;
	private String bindAddress;
	private int maxThreads = 0;
	private TServer server;
	private TServerTransport serverTransport;
	private ExecutorService servingExecutor;
	private long receivedBytes = 0l;
	private long starttime = 0l;

	public void ini() {
		LOG.info("Configuring thrift source.");
		port = 7676;
		bindAddress = "0.0.0.0";
		maxThreads = 64;
		starttime = System.currentTimeMillis();
	}

	@SuppressWarnings("rawtypes")
	public void start() throws Exception {
		LOG.info("Starting thrift server");
		maxThreads = (maxThreads <= 0) ? Integer.MAX_VALUE : maxThreads;
		Class<?> serverClass = null;
		Class<?> argsClass = null;
		TServer.AbstractServerArgs args = null;
		
		LOG.info("using TThreadPoolServer");
        try {
            serverTransport = new TServerSocket(new InetSocketAddress(
                    bindAddress, port));
            serverClass = Class.forName("org.apache.thrift.server.TThreadPoolServer");
            argsClass = Class.forName("org.apache.thrift.server.TThreadPoolServer$Args");
            args = (TServer.AbstractServerArgs) argsClass.getConstructor(
                    TServerTransport.class).newInstance(serverTransport);
            Method m = argsClass.getDeclaredMethod("maxWorkerThreads",
                    int.class);
            m.invoke(args, maxThreads);
        } catch (ClassNotFoundException e1) {
            throw new Exception(
                    "Cannot find TThreadPoolServer.", e1);
        } catch (Throwable throwable) {
            throw new Exception("Cannot start Thrift server.",
                    throwable);
        }
		try {

			args.protocolFactory(new TCompactProtocol.Factory());
			args.inputTransportFactory(new TFastFramedTransport.Factory());
			args.outputTransportFactory(new TFastFramedTransport.Factory());
			args.processor(new ThriftSourceProtocol.Processor<ThriftSourceHandler>(
					new ThriftSourceHandler()));

			server = (TServer) serverClass.getConstructor(argsClass)
					.newInstance(args);
		} catch (Throwable ex) {
			throw new Exception("Cannot start Thrift Server.", ex);
		}

		servingExecutor = Executors
				.newSingleThreadExecutor(new ThreadFactoryBuilder()
						.setNameFormat("Dayutianfei Thrift Server I/O Boss").build());

		/**
		 * Start serving.
		 */
		servingExecutor.submit(new Runnable() {
			@Override
			public void run() {
				server.serve();
			}
		});

		long timeAfterStart = System.currentTimeMillis();
		while (!server.isServing()) {
			try {
				if (System.currentTimeMillis() - timeAfterStart >= 10000) {
					throw new Exception("Thrift server failed to start!");
				}
				TimeUnit.MILLISECONDS.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new Exception(
						"Interrupted while waiting for Thrift server"
								+ " to start.", e);
			}
		}
		LOG.info("Started Thrift server.");
	}

	public void stop() throws Exception {
		if (server != null && server.isServing()) {
			server.stop();
		}
		if (servingExecutor != null) {
			servingExecutor.shutdown();
			try {
				if (!servingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
					servingExecutor.shutdownNow();
				}
			} catch (InterruptedException e) {
				throw new Exception(
						"Interrupted while waiting for server to be "
								+ "shutdown.");
			}
		}
	}

	private class ThriftSourceHandler implements ThriftSourceProtocol.Iface {

		@Override
		public Status append(ThriftFlumeEvent event) throws TException {
			receivedBytes += event.getBody().length;
			LOG.info("received total data  : " + receivedBytes 
			    + " bytes, cost: " + (System.currentTimeMillis()-starttime));
			return Status.OK;
		}

		@Override
		public Status appendBatch(List<ThriftFlumeEvent> events)
				throws TException {
			return Status.OK;
		}

		@Override
		public String getSchema(String dbName, String tblName)
				throws TException {
			return "";
		}

        @Override
        public String getQ() throws TException {
            return "";
        }
	}

}
