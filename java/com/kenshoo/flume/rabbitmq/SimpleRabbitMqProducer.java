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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: sagyr
 * Date: 7/5/11
 * Time: 7:12 PM
 */
public class SimpleRabbitMqProducer implements QueuePublisher {
    private final ConnectionFactory factory;
    private Connection conn;
    private Channel channel;
	private final String exchange;
	private final String queueName;
	private final String routingKey;

    public SimpleRabbitMqProducer(String host, String userName, String password, String exchange, String queueName, String routingKey) {
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

    public void open() throws IOException {
        conn = factory.newConnection();
        channel = conn.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
        channel.queueBind(queueName, exchange, routingKey, null);
    }

    public void close() throws IOException {
        channel.close();
        conn.close();
    }

    public void publish(String queueName, byte[] msg) throws IOException {
    	String routingKey = this.routingKey;
    	if (this.routingKey != null) {
    		routingKey = queueName;
    	}
        channel.basicPublish(exchange, routingKey, MessageProperties.TEXT_PLAIN, msg);
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
