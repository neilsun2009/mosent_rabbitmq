package main;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitModule {
	
	private static final String EXCHANGE_NAME = "rmq_direct_test";
	private static final String QUEUE_NAME = "rmq_direct_test_queue_all_1";
	private static final Logger log = LoggerFactory.getLogger(RabbitModule.class);
	
	public static void main(String[] args)
			throws java.io.IOException,
      		java.lang.InterruptedException, TimeoutException, KeyManagementException, NoSuchAlgorithmException, URISyntaxException{
		Connection connection = null;
		while (true) {
            try {
//        		declare factory and create channel
        		ConnectionFactory factory = new ConnectionFactory();
//        		  factory.setUri("amqp://admin:admin@localhost:5672/rabbit_test");
        		factory.setUsername(RabbitConfig.username);
        		factory.setPassword(RabbitConfig.password);
        		factory.setVirtualHost(RabbitConfig.vhost);
        		connection = factory.newConnection();
        		break;
            } catch (Exception e) {
                log.info("Connection broken: {}, reconnection in 5 seconds.", e.getClass().getName());
                try {
                    Thread.sleep(5000); //sleep and then try again
                } catch (InterruptedException e1) {
                    break;
                }
            }
        }
		Channel channel = connection.createChannel();
		  
		channel.exchangeDeclare(EXCHANGE_NAME, "direct");
	//		  String queueName = channel.queueDeclare().getQueue();
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		
//		int prefetchCount = 1;
//		channel.basicQos(prefetchCount);
		if (args.length < 1){
		    System.err.println("Usage: ReceiveLogsDirect [ibeacon] [wifi]");
		    System.exit(1);
		}
		StringBuilder sBuilder = new StringBuilder();
	    for (String routing : args){
	      channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, routing);
	      sBuilder.append(routing + " ");
	    }
	    log.info("Connection succeed.");
		System.out.println(" [*] Waiting for messages on < " + sBuilder.toString() + ">.");
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
	        AMQP.BasicProperties properties, byte[] body) 
	        		throws IOException {
				String message = new String(body, "UTF-8");
				String routing = envelope.getRoutingKey();
				log.info(" [x] Received '{}' from {}", message, routing);
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		};
		channel.basicConsume(QUEUE_NAME, false, consumer);
	}
}
