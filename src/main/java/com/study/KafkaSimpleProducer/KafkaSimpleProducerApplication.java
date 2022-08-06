package com.study.KafkaSimpleProducer;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class KafkaSimpleProducerApplication {
	private static final Logger log = LoggerFactory.getLogger(KafkaSimpleProducerApplication.class.getSimpleName());

	public static void main(String[] args) {
		//SpringApplication.run(KafkaSimpleProducerApplication.class, args);
		log.info("Starting Spring boot app Producer");

		//Create producer properties
		Properties producerProperties = new Properties();
		log.info(StringSerializer.class.getSimpleName());
		log.info(StringSerializer.class.getName());
		/** Pay attention to the bootstrap server address below
		 * It is not localhost though the Kafka server is running on local machine
		 * because Producer is running on windows and Kafka is running on Ubuntu on windows.
		 * Hence using Ubuntu's ip address here and in config files on cluster
 		 */
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.29.43.86:9092");
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//create producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);

		//create a producer record to send
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>("java_topic","Hello");
		//send record --asynchronous
		kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
			if(e==null){
				log.info("message sent successfully!!");
			} else{
				log.error("Msg sent failed");
				e.printStackTrace();
			}
		});

		//flush
		kafkaProducer.flush();

		// flush and close

		kafkaProducer.close();
	}
}
