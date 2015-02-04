package com.thomascook.kafkaconsumer.runnables;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import com.thomascook.kafkaconsumer.format.ElasticSearchSchemaFormateer;

public class ElasticSearchRunnable implements Runnable {
	
	private KafkaStream<byte[], byte[]> m_stream;
	private int m_threadNumber;

	private JestClient client;
	
	private String elasticIndex = "logging-index";
	private String elasticType = "logging";
	private String elasticHost = "http://localhost:9200";

	public ElasticSearchRunnable(KafkaStream<byte[], byte[]> a_stream,
			int a_threadNumber) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;

		try {
			// Configuration
			HttpClientConfig clientConfig = new HttpClientConfig.Builder(
					elasticHost).multiThreaded(true).build();

			// Construct a new Jest client according to configuration via
			// factory
			JestClientFactory factory = new JestClientFactory();
			factory.setHttpClientConfig(clientConfig);
			client = factory.getObject();
		} catch (Exception ex) {
			// Catch it
		}
	}

	

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()) {

			MessageAndMetadata<byte[], byte[]> elem = it.next();
			String msg = new String(elem.message());
			System.out.println("Thread " + m_threadNumber + ": " + msg);

			if (client != null) {
				try {
					// Set up the es index response
					String uuid = UUID.randomUUID().toString();
					Map<String, Object> data = new HashMap<String, Object>();
					ElasticSearchSchemaFormateer.write(data, msg, elem.offset());
					// insert the document into elasticsearch
					Index index = new Index.Builder(data).index(elasticIndex)
							.type(elasticType).id(uuid).build();
					client.execute(index);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}