package ca.bc.gov.mal.pit.msg.queue.example;

import io.nats.client.AuthHandler;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.NKey;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.Status;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Properties;

public class NatsPublish {

	public static void main(String[] args) {

		InputStream ins = null;
		try {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			ins = cl.getResourceAsStream("msg-queue-dev.properties");
			
			Properties props = new Properties();
			props.load(ins);
			
			String server = props.getProperty("msg.queue.server");
			String seed = props.getProperty("msg.queue.publisher.nkey.seed");
			String subject = props.getProperty("msg.queue.subject");

			if ( server == null || subject == null || seed == null ) {
				throw new IllegalArgumentException("Required property is missing");
			}

			String data = "Testing, testing, 1, 2, 3";

	        System.out.printf("\nPublishing to %s. Server is %s\n\n", subject, server);

	        AuthHandler authHandler = new AuthHandler() {
				
	            private final NKey nkey = NKey.fromSeed(seed.toCharArray());

	            @Override
				public byte[] sign(byte[] nonce) {
	                try {
	                    return this.nkey.sign(nonce);
	                } catch (GeneralSecurityException|IOException|NullPointerException ex) {
	                    return null;
	                }
				}
				
				@Override
				public char[] getJWT() {
					return null;
				}
				
				@Override
				public char[] getID() {
			        try {
			            return this.nkey.getPublicKey();
			        } catch (GeneralSecurityException|IOException|NullPointerException ex) {
			            return null;
			        }
				}
			};

			ConnectionListener connListener = new ConnectionListener() {
				@Override
				public void connectionEvent(Connection conn, Events type) {
					System.out.println("Connection Event: " + type);
				}
				
				@Override
				public void connectionEvent(Connection conn, Events type, Long time, String uriDetails) {
					System.out.println("Connection Event: " + type + ", URI: " + uriDetails);
				}
			};
			
			ErrorListener errListener = new ErrorListener() {

				@Override
				public void errorOccurred(Connection conn, String error) {
					System.out.println(supplyMessage("errorOccurred", conn, null, null, "Error: ", error));
				}

				@Override
				public void exceptionOccurred(Connection conn, Exception exp) {
					System.out.println(supplyMessage("exceptionOccurred", conn, null, null, "Exception: ", exp));
				}

				@Override
				public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
					System.out.println(supplyMessage("flowControlProcessed", conn, null, sub, "Subject:", subject, "FlowControlSource:", source));
				}

				@Override
				public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
					System.out.println(supplyMessage("heartbeatAlarm", conn, null, sub, "lastStreamSequence: ", lastStreamSequence, "lastConsumerSequence: ", lastConsumerSequence));
				}
				
				@Override
				public void messageDiscarded(Connection conn, Message msg) {
					System.out.println(supplyMessage("messageDiscarded", conn, null, null, "Message: ", msg));
				}

				@Override
				public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
					System.out.println(supplyMessage("pullStatusError", conn, null, sub, "Status:", status));
				}
				
				@Override
				public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
					System.out.println(supplyMessage("pullStatusWarning", conn, null, sub, "Status:", status));
				}
				
				@Override
				public void slowConsumerDetected(Connection conn, Consumer consumer) {
					System.out.println(supplyMessage("slowConsumerDetected", conn, consumer, null));
				}

				@Override
				public void socketWriteTimeout(Connection conn) {
					System.out.println(supplyMessage("socketWriteTimeout", conn, null, null));
				}

				@Override
				public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
					System.out.println(supplyMessage("unhandledStatus", conn, null, sub, "Status:", status));
				}
			};			
			
	        Options.Builder builder = new Options.Builder()
	                .server(server)
	                .connectionTimeout(Duration.ofSeconds(10))
	                .pingInterval(Duration.ofSeconds(30))
	                .reconnectWait(Duration.ofSeconds(5))
	                .authHandler(authHandler)
	                .connectionListener(connListener)
	                .errorListener(errListener)
	                .maxReconnects(-1);
			
	        Options options = builder.build();
	
	        try (Connection nc = Nats.connect(options)) {
	
	            JetStream js = nc.jetStream();
	
	            Message msg = NatsMessage.builder()
	                    .subject(subject)
	                    .data(data, StandardCharsets.UTF_8)
	                    .build();

	            PublishAck pa = js.publish(msg);
	            System.out.printf("Published message %s on subject %s, stream %s, seqno %d, has error %s. \n",
	                   data, subject, pa.getStream(), pa.getSeqno(), pa.hasError() ? "Yes" : "No");
	            
	        } catch (Exception e) {
	                e.printStackTrace();
	        }

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if ( ins != null ) {
					ins.close();
					ins = null;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
}
