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
		RabbitMqSink eventSink = (RabbitMqSink) rabbitMqSinkBuilder.build(context, publisher);
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
		RabbitMqSink eventSink = (RabbitMqSink) rabbitMqSinkBuilder.build(context, publisher,
				queueDomain, userName, password, exchange);
		assertEquals(RabbitMqSink.class.getSimpleName(), eventSink.getName());
	}
}
