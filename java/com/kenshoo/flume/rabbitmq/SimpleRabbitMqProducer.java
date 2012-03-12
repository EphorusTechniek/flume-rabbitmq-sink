/*
 * Copyright 2011 Kenshoo.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kenshoo.flume.rabbitmq;

import java.io.IOException;
import java.nio.charset.Charset;

import com.cloudera.flume.core.Event;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * Created by IntelliJ IDEA. User: sagyr Date: 7/5/11 Time: 7:12 PM
 */
public class SimpleRabbitMqProducer implements QueuePublisher {
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private final ConnectionFactory factory;
	private Connection conn;
	private Channel channel;
	private final String exchange;
	private final String queueName;
	private final String routingKey;

	public SimpleRabbitMqProducer(String host, String userName, String password, String exchange,
			String queueName, String routingKey) {
		this.exchange = exchange;
		this.queueName = queueName;
		this.routingKey = routingKey;
		factory = new ConnectionFactory();
		factory.setUsername(userName);
		factory.setPassword(password);
		factory.setVirtualHost("/");
		factory.setHost(host);
		factory.setPort(5672);
	}

	@Override
	public void open() throws IOException {
		conn = createConnection();
		channel = createChannel();
		channel.queueDeclare(queueName, false, false, false, null);
		channel.queueBind(queueName, exchange, routingKey, null);
	}

	Connection createConnection() throws IOException {
		return factory.newConnection();
	}

	Channel createChannel() throws IOException {
		return conn.createChannel();
	}

	@Override
	public void close() throws IOException {
		channel.close();
		conn.close();
	}

	@Override
	public void publish(String queueName, byte[] msg) throws IOException {
		String routingKey = this.routingKey;
		if (this.routingKey == null) {
			routingKey = queueName;
		}
		channel.basicPublish(exchange, routingKey, MessageProperties.TEXT_PLAIN, msg);
	}

	@Override
	public void publish(String queueName, Event e) throws IOException {
		String routingKey = this.routingKey;
		if (this.routingKey == null) {
			routingKey = queueName;
		}
		StringBuffer buf = new StringBuffer();
		for (String key : e.getAttrs().keySet()) {
			buf.append("X-Flume-").append(key).append(':').append(new String(e.get(key), UTF8))
					.append('|');
		}
		buf.append(new String(e.getBody(), UTF8));
		channel.basicPublish(exchange, routingKey, MessageProperties.TEXT_PLAIN, buf.toString()
				.getBytes(UTF8));
	}

	public ConnectionFactory getFactory() {
		return factory;
	}

	public String getExchange() {
		return exchange;
	}

	public String getQueueName() {
		return queueName;
	}

	public String getRoutingKey() {
		return routingKey;
	}
}
