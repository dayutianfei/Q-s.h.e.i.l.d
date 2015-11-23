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
package cn.dayutianfei.loadserver.server.source;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceConfigurationConstants;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.flume.source.http.JSONHandler;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.client.message.DataMessage;
import cn.dayutianfei.loadserver.server.source.http.handler.CacheOperaServlet;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;


/**
 * A source which accepts Flume Events by HTTP POST and GET. GET should be used
 * for experimentation only. HTTP requests are converted into flume events by a
 * pluggable "handler" which must implement the {@linkplain HTTPSourceHandler}
 * interface. This handler takes a {@linkplain HttpServletRequest} and returns a
 * list of flume events.
 * 
 * The source accepts the following parameters:
 * <p>
 * <tt>port</tt>: port to which the server should bind. Mandatory
 * <p>
 * <tt>handler</tt>: the class that deserializes a HttpServletRequest into a
 * list of flume events. This class must implement HTTPSourceHandler. Default:
 * {@linkplain JSONHandler}.
 * <p>
 * <tt>handler.*</tt> Any configuration to be passed to the handler.
 * <p>
 * 
 * All events deserialized from one Http request are committed to the channel in
 * one transaction, thus allowing for increased efficiency on channels like the
 * file channel. If the handler throws an exception this source will return a
 * HTTP status of 400. If the channel is full, or the source is unable to append
 * events to the channel, the source will return a HTTP 503 - Temporarily
 * unavailable status.
 * 
 * A JSON handler which converts JSON objects to Flume events is provided.
 * 
 */
public class HttpSource extends AbstractSource implements EventDrivenSource, Configurable {
    /*
     * There are 2 ways of doing this: a. Have a static server instance and use
     * connectors in each source which binds to the port defined for that
     * source. b. Each source starts its own server instance, which binds to the
     * source's port.
     * 
     * b is more efficient than a because Jetty does not allow binding a servlet
     * to a connector. So each request will need to go through each each of the
     * handlers/servlet till the correct one is found.
     */

    private static final Logger LOG = LoggerFactory.getLogger(HttpSource.class);
    private volatile Integer port;
    private volatile Server srv;
    private volatile String host;
    private HTTPSourceHandler handler;
    private SourceCounter sourceCounter;

    // SSL configuration variable
    private volatile String keyStorePath;
    private volatile String keyStorePassword;
    private volatile Boolean sslEnabled;


    @Override
    public void configure(Context context) {
        try {
            // SSL related config
            sslEnabled = context.getBoolean(HTTPSourceConfigurationConstants.SSL_ENABLED, false);
            port = context.getInteger(HTTPSourceConfigurationConstants.CONFIG_PORT);
            host =  context.getString(HTTPSourceConfigurationConstants.CONFIG_BIND,
                        HTTPSourceConfigurationConstants.DEFAULT_BIND);

            Preconditions.checkState(host != null && !host.isEmpty(),
                "HTTPSource hostname specified is empty");
            Preconditions.checkNotNull(port, "HTTPSource requires a port number to be" + " specified");

            String handlerClassName =
                    context.getString(HTTPSourceConfigurationConstants.CONFIG_HANDLER,
                        HTTPSourceConfigurationConstants.DEFAULT_HANDLER).trim();

            if (sslEnabled) {
                LOG.debug("SSL configuration enabled");
                keyStorePath = context.getString(HTTPSourceConfigurationConstants.SSL_KEYSTORE);
                Preconditions.checkArgument(keyStorePath != null && !keyStorePath.isEmpty(),
                    "Keystore is required for SSL Conifguration");
                keyStorePassword = context.getString(HTTPSourceConfigurationConstants.SSL_KEYSTORE_PASSWORD);
                Preconditions.checkArgument(keyStorePassword != null,
                    "Keystore password is required for SSL Configuration");
            }

            @SuppressWarnings("unchecked")
            Class<? extends HTTPSourceHandler> clazz =
                    (Class<? extends HTTPSourceHandler>) Class.forName(handlerClassName);
            handler = clazz.getDeclaredConstructor().newInstance();
            Map<String, String> subProps =
                    context.getSubProperties(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX);
            handler.configure(new Context(subProps));
        }
        catch (ClassNotFoundException ex) {
            LOG.error("Error while configuring HTTPSource. Exception follows.", ex);
            Throwables.propagate(ex);
        }
        catch (ClassCastException ex) {
            LOG.error("Deserializer is not an instance of HTTPSourceHandler."
                    + "Deserializer must implement HTTPSourceHandler.");
            Throwables.propagate(ex);
        }
        catch (Exception ex) {
            LOG.error("Error configuring HTTPSource!", ex);
            Throwables.propagate(ex);
        }
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @Override
    public void start() {
        Preconditions.checkState(null == srv, "Running HTTP Server found in source: " + getName()
                + " before I started one." + "Will not attempt to start.");
        srv = new Server();

        // Connector Array
        Connector[] connectors = new Connector[1];

        if (sslEnabled) {
            SslSocketConnector sslSocketConnector = new SslSocketConnector();
            sslSocketConnector.setKeystore(keyStorePath);
            sslSocketConnector.setKeyPassword(keyStorePassword);
            sslSocketConnector.setReuseAddress(true);
            connectors[0] = sslSocketConnector;
        }
        else {
            SelectChannelConnector connector = new SelectChannelConnector();
            connector.setReuseAddress(true);
            connectors[0] = connector;
        }

        connectors[0].setHost(host);
        connectors[0].setPort(port);
        srv.setConnectors(connectors);
        try {
            org.mortbay.jetty.servlet.Context root =
                    new org.mortbay.jetty.servlet.Context(srv, "/",
                        org.mortbay.jetty.servlet.Context.SESSIONS);
            root.addServlet(new ServletHolder(new DLoadingServlet()), "/load");
            root.addServlet(new ServletHolder(new KingHTTPServlet()), "/king");
            root.addServlet(new ServletHolder(new CacheOperaServlet()), "/cache");
            srv.start();
            Preconditions.checkArgument(srv.getHandler().equals(root));
        }
        catch (Exception ex) {
            LOG.error("Error while starting HTTPSource. Exception follows.", ex);
            Throwables.propagate(ex);
        }
        Preconditions.checkArgument(srv.isRunning());
        sourceCounter.start();
        super.start();
        LOG.info("HTTP source {} started.", getName());
    }


    @Override
    public void stop() {
        try {
            srv.stop();
            srv.join();
            srv = null;
        }
        catch (Exception ex) {
            LOG.error("Error while stopping HTTPSource. Exception follows.", ex);
        }
        sourceCounter.stop();
        LOG.info("Http source {} stopped. Metrics: {}", getName(), sourceCounter);
    }

    /**
     * 用于进行相关资源的探测
     * @author wzy
     */
    private class KingHTTPServlet extends HttpServlet {
        private static final long serialVersionUID = 4891924863218790344L;

        @Override
        public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
            List<Event> events = Collections.emptyList(); // create empty list
            try {
                events = handler.getEvents(request);
            }
            catch (HTTPBadRequestException ex) {
                LOG.warn("Received bad request from client. ", ex);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Bad request from client. " + ex.getMessage());
                return;
            }
            catch (Exception ex) {
                LOG.warn("Deserializer threw unexpected exception. ", ex);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Deserializer threw unexpected exception. " + ex.getMessage());
                return;
            }
            sourceCounter.incrementAppendBatchReceivedCount();
            sourceCounter.addToEventReceivedCount(events.size());
            try {
                getChannelProcessor().processEventBatch(events);
            }
            catch (ChannelException ex) {
                LOG.warn("Error appending event to channel. "
                        + "Channel might be full. Consider increasing the channel "
                        + "capacity or make sure the sinks perform faster.", ex);
                response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "Error appending event to channel. Channel might be full." + ex.getMessage());
                return;
            }
            catch (Exception ex) {
                LOG.warn("Unexpected error appending event to channel. ", ex);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Unexpected error while appending event to channel. " + ex.getMessage());
                return;
            }
            response.setCharacterEncoding(request.getCharacterEncoding());
            response.setStatus(HttpServletResponse.SC_OK);
            response.flushBuffer();
            sourceCounter.incrementAppendBatchAcceptedCount();
            sourceCounter.addToEventAcceptedCount(events.size());
        }


        @Override
        public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
            doPost(request, response);
        }
    }

    
    /**
     * 用于进行加载数据的处理
     * @author wzy
     */
    private class DLoadingServlet extends HttpServlet {
        private static final long serialVersionUID = 1L;

        @Override
        public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
            /*
             * Get headers from http query string, and get body from http post
             * content.
             */
            Map<String, String> headers = Maps.newHashMap();
            headers.put(DataMessage.HEAD_DB,  request.getHeader(DataMessage.HEAD_DB));
            headers.put(DataMessage.HEAD_TBL, request.getHeader(DataMessage.HEAD_TBL));
            headers.put(DataMessage.HEAD_OP, request.getHeader(DataMessage.HEAD_OP));
            headers.put(DataMessage.HEAD_CHANNEL, request.getHeader(DataMessage.HEAD_CHANNEL));
            int totalLength = request.getContentLength();
            if (totalLength < 0) {
                LOG.error("content length: " + totalLength);
                response.sendError(HttpServletResponse.SC_NO_CONTENT);
                return;
            }
            ServletInputStream input = request.getInputStream();
            byte[] body = new byte[totalLength];
            int position = 0;
            do {
                position += input.read(body, position, totalLength - position);
            } while (position < totalLength);
            sourceCounter.incrementAppendBatchReceivedCount();
            sourceCounter.addToEventReceivedCount(1);
            try {
                getChannelProcessor().processEvent(EventBuilder.withBody(body, headers));
            }
            catch (ChannelException ex) {
                LOG.warn("Error appending event to channel. "
                        + "Channel might be full. Consider increasing the channel "
                        + "capacity or make sure the sinks perform faster.", ex);
                response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "Error appending event to channel. Channel might be full." + ex.getMessage());
                return;
            }
            catch (Exception ex) {
                LOG.warn("Unexpected error appending event to channel. ", ex);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Unexpected error while appending event to channel. " + ex.getMessage());
                return;
            }
            response.setCharacterEncoding(request.getCharacterEncoding());
            response.setStatus(HttpServletResponse.SC_OK);
            response.flushBuffer();
            sourceCounter.incrementAppendBatchAcceptedCount();
            sourceCounter.addToEventAcceptedCount(1);
        }

        @Override
        public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
            doPost(request, response);
        }
    }
}
