package ca.bc.gov.mal.pit.msg.queue.example;

import io.nats.client.AuthHandler;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.NKey;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Properties;

public class NatsPublish {

	public static void main(String[] args) {

		try {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			InputStream ins = cl.getResourceAsStream("msg-queue-dev.properties");
			
			Properties props = new Properties();
			props.load(ins);
			
			String server = props.getProperty("msg.queue.server");
//			String userName = props.getProperty("msg.queue.publisher.user");
//			String password = props.getProperty("msg.queue.publisher.password");
			String seed = props.getProperty("msg.queue.publisher.nkey.seed");
			String subject = props.getProperty("msg.queue.subject");

			if ( server == null || subject == null || seed == null ) {
				throw new IllegalArgumentException("Required property is missing");
			}

			String data = "Testing, testing, 7, 8, 9";

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
	        
	        Options.Builder builder = new Options.Builder()
	                .server(server)
	                .connectionTimeout(Duration.ofSeconds(5))
	                .pingInterval(Duration.ofSeconds(10))
	                .reconnectWait(Duration.ofSeconds(1))
//	                .userInfo(userName, password)
	                .authHandler(authHandler)
	// TODO: Do we need this?
	//                .connectionListener(null)
	//                .errorListener(el);
	                .maxReconnects(-1);
			
	        Options options = builder.build();
	
	        try (Connection nc = Nats.connect(options)) {
	
	            JetStream js = nc.jetStream();
	
	            Message msg = NatsMessage.builder()
	                    .subject(subject)
	                    .data(data, StandardCharsets.UTF_8)
	                    .build();

	            // TODO: Might need better error handling here. Docs suggest you need to manaully check for
	            // errors.
	            PublishAck pa = js.publish(msg);
	            System.out.printf("Published message %s on subject %s, stream %s, seqno %d, has error %s. \n",
	                   data, subject, pa.getStream(), pa.getSeqno(), pa.hasError() ? "Yes" : "No");

	            // TODO: Do we need this?
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
