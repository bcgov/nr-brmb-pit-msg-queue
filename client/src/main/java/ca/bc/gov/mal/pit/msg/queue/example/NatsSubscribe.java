package ca.bc.gov.mal.pit.msg.queue.example;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class NatsSubscribe {

	public static void main(String[] args) {

		try {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			InputStream ins = cl.getResourceAsStream("msg-queue-dev.properties");
			
			Properties props = new Properties();
			props.load(ins);
			
			String server = props.getProperty("msg.queue.server");
			String userName = props.getProperty("msg.queue.subscriber.user");
			String password = props.getProperty("msg.queue.subscriber.password");
			String subject = props.getProperty("msg.queue.subject");
			String consumer = props.getProperty("msg.queue.subscriber.consumer");


			if ( server == null || userName == null || password == null || subject == null || consumer == null ) {
				throw new IllegalArgumentException("Required property is missing");
			}

	        System.out.printf("\nSubscribing to %s. Server is %s\n\n", consumer, server);
		
	        Options.Builder builder = new Options.Builder()
	                .server(server)
	                .connectionTimeout(Duration.ofSeconds(5))
	                .pingInterval(Duration.ofSeconds(10))
	                .reconnectWait(Duration.ofSeconds(1))
	                .userInfo(userName, password)
	// TODO: Do we need this?
	//                .connectionListener(null)
	//                .errorListener(el);
	                .maxReconnects(-1);
			
	        Options options = builder.build();
	
	        try (Connection nc = Nats.connect(options)) {

	            JetStream js = nc.jetStream();

// TODO: Looks like this is not needed, since the consumer already exists and should not be modified.
//	            ConsumerConfiguration cc = ConsumerConfiguration.builder()
//	                    .ackWait(Duration.ofMillis(2500))
//	                    .build();
	            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
	                    .durable(consumer)
	                    .stream(subject)
//	                    .configuration(cc)  // TODO: Do we need this?
	                    .bind(true)
	                    .build();

	            // TODO: Is it necessary to pass the subject here?
	            JetStreamSubscription sub = js.subscribe(null, pullOptions);

	            nc.flush(Duration.ofSeconds(1));

	            // TODO: Is this the right method for fetching messages?
                List<Message> messageList = sub.fetch(1, Duration.ofSeconds(1));
                if ( messageList != null && !messageList.isEmpty() ) {
                	Message msg = messageList.get(0);
                	System.out.println("Received message for " + subject + ": " + msg);
                	msg.ack();
                } else {
                	System.out.println("Nothing received");
                }

	            // TODO: Do we need these?
	            sub.unsubscribe();
	            nc.close();
	        } catch (Exception e) {
	                e.printStackTrace();
	        }

	        ins.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
