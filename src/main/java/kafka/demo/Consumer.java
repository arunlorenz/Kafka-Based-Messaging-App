package kafka.demo;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public final class Consumer {
    public static void main(String[] args) throws URISyntaxException
	{
        String TOPIC_NAME = "Topic";
        String TRUSTSTORE_PASSWORD = "PASS";

        String sasl_username = "admin";
        String sasl_password = "PASS";
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);

        URL truststoreUrl = Consumer.class.getClassLoader().getResource("client.truststore.jks");
        if (truststoreUrl == null)
        {
            throw new RuntimeException("Truststore file not found in classpath.");
        }

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "demo.aivencloud.com:11722");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("sasl.jaas.config", jaasConfig);
        properties.setProperty("ssl.endpoint.identification.algorithm", "");
        properties.setProperty("ssl.truststore.type", "jks");
        properties.setProperty("ssl.truststore.location", new File(truststoreUrl.toURI()).getPath());
        properties.setProperty("ssl.truststore.password", TRUSTSTORE_PASSWORD);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "groupid");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(100));
            messages.forEach(message -> {
                System.out.println("Received message: " + message.value());
            });
        }
    }
}
