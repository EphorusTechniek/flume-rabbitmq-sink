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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory;
import com.cloudera.flume.core.EventSink;

/**
 * Created by IntelliJ IDEA. User: sagyr Date: 8/2/11 Time: 7:23 PM To change
 * this template use File | Settings | File Templates.
 */
class RabbitMqSinkBuilder extends SinkFactory.SinkBuilder {
	private static final String PN_ROUTING_KEY = "routingKey";
	private static final String PN_QUEUE_NAME = "queueName";
	private static final String PN_EXCHANGE = "exchange";
	private static final String PN_PASSWORD = "password";
	private static final String PN_USER_NAME = "userName";
	private static final String PN_QUEUE_DOMAIN = "queueDomain";
	private static final String PN_FORMAT_ATTR = "";

	@SuppressWarnings("static-access")
	private Options createOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("host") //
				.withDescription("The RabbitMQ host to connect to") //
				.isRequired() //
				.hasArg() //
				.withArgName("host") //
				.create('h'));
		options.addOption(OptionBuilder.withLongOpt(PN_QUEUE_DOMAIN) //
				.withDescription("The domain to add to the host") //
				.hasArg() //
				.withArgName("domain") //
				.create('d'));
		options.addOption(OptionBuilder.withLongOpt(PN_USER_NAME) //
				.withDescription("The username to connect to RabbitMQ") //
				.hasArg() //
				.withArgName("name") //
				.create('u'));
		options.addOption(OptionBuilder.withLongOpt(PN_PASSWORD) //
				.withDescription("The password to connect to RabbitMQ") //
				.hasArg() //
				.withArgName("password") //
				.create('p'));
		options.addOption(OptionBuilder.withLongOpt(PN_EXCHANGE) //
				.withDescription("The name of the exchange in the RabbitMQ") //
				.hasArg() //
				.withArgName("name") //
				.create('e'));
		options.addOption(OptionBuilder.withLongOpt(PN_QUEUE_NAME) //
				.withDescription("The name of the queue in the RabbitMQ") //
				.hasArg() //
				.withArgName("name") //
				.create('q'));
		options.addOption(OptionBuilder.withLongOpt(PN_ROUTING_KEY) //
				.withDescription("The routing key of the message") //
				.hasArg() //
				.withArgName("key") //
				.create('r'));
		options.addOption(OptionBuilder.withLongOpt(PN_FORMAT_ATTR) //
				.withDescription("Format the attributes of the event like: X-Flume-attr: value|") //
				.create('f'));
		return options;
	}

	class StringHelpFormatter extends HelpFormatter {
		public String getHelpMessage(Options options) {
			StringBuffer sb = new StringBuffer();

			renderOptions(sb, 5000, options, 0, 0);
			return sb.toString();
		}
	}

	@Override
	public EventSink build(Context context, String... args) {
		Options options = createOptions();
		CommandLineParser parser = new PosixParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			String publisher = cmd.getOptionValue("host");
			String queueDomain = cmd.hasOption(PN_QUEUE_DOMAIN) ? cmd
					.getOptionValue(PN_QUEUE_DOMAIN) : "";
			String userName = cmd.hasOption(PN_USER_NAME) ? cmd.getOptionValue(PN_USER_NAME)
					: "guest";
			String password = cmd.hasOption(PN_PASSWORD) ? cmd.getOptionValue(PN_PASSWORD)
					: "guest";
			String exchange = cmd.hasOption(PN_EXCHANGE) ? cmd.getOptionValue(PN_EXCHANGE) : "";
			String queueName = cmd.hasOption(PN_QUEUE_NAME) ? cmd.getOptionValue(PN_QUEUE_NAME)
					: null;
			String routingKey = cmd.hasOption(PN_ROUTING_KEY) ? cmd.getOptionValue(PN_ROUTING_KEY)
					: null;
			boolean formatAttributes = cmd.hasOption(PN_FORMAT_ATTR);
			return new RabbitMqSink(new SimpleRabbitMqProducer(publisher, userName, password,
					exchange, queueName, routingKey), queueDomain, formatAttributes);
		} catch (ParseException e) {
			StringHelpFormatter formatter = new StringHelpFormatter();
			throw new IllegalArgumentException(e.getMessage() + ", synopsys is: "
					+ formatter.getHelpMessage(options).replaceAll("\n", ", "));
		}
	}
}
