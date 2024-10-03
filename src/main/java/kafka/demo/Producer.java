package kafka.demo;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
import java.util.Scanner;
import java.io.File;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class Producer
{
    public static void main(String[] args) throws URISyntaxException
    {
        String TOPIC_NAME = "Topic";
        String TRUSTSTORE_PASSWORD = "PASS";

        String sasl_username = "admin";
        String sasl_password = "PASS";
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);

        URL truststoreUrl = Producer.class.getClassLoader().getResource("client.truststore-3.jks");
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
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter your name/ID: ");
        String producerId = scanner.nextLine();

        while (true)
        {
            System.out.print("Enter message to send: ");
            String message = scanner.nextLine();
            String formattedMessage = producerId + ": " + message;
            producer.send(new ProducerRecord<>(TOPIC_NAME, formattedMessage));
            System.out.println("Message sent: " + formattedMessage);
        }
    }
}
