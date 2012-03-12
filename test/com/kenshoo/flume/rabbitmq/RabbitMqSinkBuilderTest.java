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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.cloudera.flume.conf.Context;

@RunWith(JMock.class)
public class RabbitMqSinkBuilderTest {
	private Mockery mockery;

	@Test
	public void testBuildWithNoParameters() {
		mockery = new Mockery() {
			{
				setImposteriser(ClassImposteriser.INSTANCE);
			}
		};
		RabbitMqSinkBuilder rabbitMqSinkBuilder = new RabbitMqSinkBuilder();
		Context context = mockery.mock(Context.class);
		String publisher = "publisher-" + Math.random();
		RabbitMqSink eventSink = (RabbitMqSink) rabbitMqSinkBuilder.build(context, "-h", publisher);
		assertEquals(RabbitMqSink.class.getSimpleName(), eventSink.getName());
	}

	@Test
	public void testBuildWithAllParameters() {
		mockery = new Mockery() {
			{
				setImposteriser(ClassImposteriser.INSTANCE);
			}
		};
		RabbitMqSinkBuilder rabbitMqSinkBuilder = new RabbitMqSinkBuilder();
		Context context = mockery.mock(Context.class);
		String publisher = "publisher-" + Math.random();
		String queueDomain = "queueDomain-" + Math.random();
		String userName = "userName-" + Math.random();
		String password = "password-" + Math.random();
		String exchange = "exchange-" + Math.random();
		String queueName = "queueName-" + Math.random();
		String routingKey = "routingKey-" + Math.random();

		RabbitMqSink eventSink = (RabbitMqSink) rabbitMqSinkBuilder.build(context, "-h", publisher,
				"-d", queueDomain, "-u", userName, "-p", password, "-e", exchange, "-q", queueName,
				"-r", routingKey);

		assertEquals(RabbitMqSink.class.getSimpleName(), eventSink.getName());
		assertEquals(queueDomain, eventSink.getQueueDomain());

		SimpleRabbitMqProducer producer = (SimpleRabbitMqProducer) eventSink.getRabbitMqProducer();
		assertEquals(userName, producer.getFactory().getUsername());
		assertEquals(password, producer.getFactory().getPassword());
		assertEquals(exchange, producer.getExchange());
		assertEquals(queueName, producer.getQueueName());
		assertEquals(routingKey, producer.getRoutingKey());
	}

	@Test
	public void testBuildWithSomeParametersUseDefaults() {
		mockery = new Mockery() {
			{
				setImposteriser(ClassImposteriser.INSTANCE);
			}
		};
		RabbitMqSinkBuilder rabbitMqSinkBuilder = new RabbitMqSinkBuilder();
		Context context = mockery.mock(Context.class);
		String publisher = "publisher-" + Math.random();
		String queueDomain = "queueDomain-" + Math.random();
		String password = "password-" + Math.random();
		String queueName = "queueName-" + Math.random();
		String routingKey = "routingKey-" + Math.random();

		RabbitMqSink eventSink = (RabbitMqSink) rabbitMqSinkBuilder.build(context, "-h", publisher,
				"-d", queueDomain, "-p", password, "-q", queueName, "-r", routingKey);

		assertEquals(RabbitMqSink.class.getSimpleName(), eventSink.getName());
		assertEquals(queueDomain, eventSink.getQueueDomain());
		assertFalse(eventSink.doFormatAttributes());

		SimpleRabbitMqProducer producer = (SimpleRabbitMqProducer) eventSink.getRabbitMqProducer();
		assertEquals("guest", producer.getFactory().getUsername());
		assertEquals(password, producer.getFactory().getPassword());
		assertEquals("", producer.getExchange());
		assertEquals(queueName, producer.getQueueName());
		assertEquals(routingKey, producer.getRoutingKey());
	}

	@Test
	public void testBuildWithFormatAttributes() {
		mockery = new Mockery() {
			{
				setImposteriser(ClassImposteriser.INSTANCE);
			}
		};
		RabbitMqSinkBuilder rabbitMqSinkBuilder = new RabbitMqSinkBuilder();
		Context context = mockery.mock(Context.class);
		String publisher = "publisher-" + Math.random();
		String queueDomain = "queueDomain-" + Math.random();
		String password = "password-" + Math.random();
		String queueName = "queueName-" + Math.random();
		String routingKey = "routingKey-" + Math.random();

		RabbitMqSink eventSink = (RabbitMqSink) rabbitMqSinkBuilder.build(context, "-h", publisher,
				"-d", queueDomain, "-p", password, "-q", queueName, "-r", routingKey, "-f");

		assertEquals(RabbitMqSink.class.getSimpleName(), eventSink.getName());
		assertEquals(queueDomain, eventSink.getQueueDomain());
		assertTrue(eventSink.doFormatAttributes());

		SimpleRabbitMqProducer producer = (SimpleRabbitMqProducer) eventSink.getRabbitMqProducer();
		assertEquals("guest", producer.getFactory().getUsername());
		assertEquals(password, producer.getFactory().getPassword());
		assertEquals("", producer.getExchange());
		assertEquals(queueName, producer.getQueueName());
		assertEquals(routingKey, producer.getRoutingKey());
	}
}
