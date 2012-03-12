package com.kenshoo.flume.rabbitmq;

import java.io.IOException;
import java.nio.charset.Charset;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventBaseImpl;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

@RunWith(JMock.class)
public class SimpleRabbitMqProducerTest {
	private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

	private Mockery mockery;

	@Before
	public void setUp() {
		mockery = new Mockery() {
			{
				setImposteriser(ClassImposteriser.INSTANCE);
			}
		};
	}

	@Test
	public void testPublishWhenRoutingKeyIsSpecified() throws IOException {
		final String exchange = "exchange-" + Math.random();
		final String queueName = "queueName-" + Math.random();
		final String routingKey = "routingKey-" + Math.random();
		final byte[] msg = new byte[] {};

		final Channel channel = mockery.mock(Channel.class);
		SimpleRabbitMqProducer producer = createProducer(channel, exchange, queueName, routingKey,
				null, false, msg);

		producer.open();
		producer.publish(null, msg);
	}

	@Test
	public void testPublishWhenNoRoutingKeyIsSpecified() throws IOException {
		String exchange = "exchange-" + Math.random();
		String queueName = "queueName-" + Math.random();
		String routingKey = null;
		byte[] msg = new byte[] {};

		String queueDomain = "queueDomain-" + Math.random();

		final Channel channel = mockery.mock(Channel.class);
		SimpleRabbitMqProducer producer = createProducer(channel, exchange, queueName, routingKey,
				queueDomain, false, msg);

		producer.open();
		producer.publish(queueDomain, msg);
	}

	@Test
	public void testPublishWithEvent() throws IOException {
		final String exchange = "exchange-" + Math.random();
		final String queueName = "queueName-" + Math.random();
		final String routingKey = null;
		final String body = "Some message";
		final byte[] msg = ("X-Flume-key2:value2|" + "X-Flume-key1:value1|" + body)
				.getBytes(CHARSET_UTF8);

		final String queueDomain = "queueDomain-" + Math.random();

		final Channel channel = mockery.mock(Channel.class);
		mockery.checking(new Expectations() {
			{
				one(channel).queueDeclare(queueName, false, false, false, null);
				one(channel).queueBind(queueName, exchange, routingKey, null);

				one(channel).basicPublish(exchange, routingKey == null ? queueDomain : routingKey,
						MessageProperties.TEXT_PLAIN, msg);
			}
		});
		SimpleRabbitMqProducer producer = new SimpleRabbitMqProducer("xxx", null, null, exchange,
				queueName, routingKey) {
			@Override
			Connection createConnection() throws IOException {
				return mockery.mock(Connection.class);
			}

			@Override
			Channel createChannel() throws IOException {
				return channel;
			}
		};

		producer.open();
		Event event = new EventBaseImpl() {
			@Override
			public byte[] getBody() {
				return body.getBytes(CHARSET_UTF8);
			}

			@Override
			public String getHost() {
				return null; // Unused
			}

			@Override
			public long getNanos() {
				return 0; // Unused
			}

			@Override
			public Priority getPriority() {
				return null; // Unused
			}

			@Override
			public long getTimestamp() {
				return 0; // Unused
			}

		};
		event.set("key1", "value1".getBytes(CHARSET_UTF8));
		event.set("key2", "value2".getBytes(CHARSET_UTF8));
		producer.publish(queueDomain, event);
	}

	private SimpleRabbitMqProducer createProducer(final Channel channel, final String exchange,
			final String queueName, final String routingKey, final String queueDomain,
			boolean includeField, final byte[] msg) throws IOException {
		mockery.checking(new Expectations() {
			{
				one(channel).queueDeclare(queueName, false, false, false, null);
				one(channel).queueBind(queueName, exchange, routingKey, null);

				one(channel).basicPublish(exchange, routingKey == null ? queueDomain : routingKey,
						MessageProperties.TEXT_PLAIN, msg);
			}
		});
		SimpleRabbitMqProducer producer = new SimpleRabbitMqProducer("xxx", null, null, exchange,
				queueName, routingKey) {
			@Override
			Connection createConnection() throws IOException {
				return mockery.mock(Connection.class);
			}

			@Override
			Channel createChannel() throws IOException {
				return channel;
			}
		};
		return producer;
	}
}
