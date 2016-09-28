package sarkkom.paragon.rmq.subscribers;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

public class ReceiverQ2 implements MessageListener {

	public void onMessage(Message message) {
		String str = (new String(message.getBody()));
		System.out.println(
				System.currentTimeMillis()
						+ "  Receiver (ReceiverQ2) : <" + str + ">"
						+ " [ receiver #-" + this.hashCode()
						+ ", " + Thread.currentThread().getId()
						+ ", " + Thread.currentThread().getName()
						+ "]");
		if (str.equals("DIRTY MESSAGE")) {
			throw new RuntimeException(" cannot process payload - " + str);
		}

		try {
			// sleep for 50 millis (test only)
			Thread.sleep(50);
		} catch (Exception e) {}
	}
}
