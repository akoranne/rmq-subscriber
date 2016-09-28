package sarkkom.paragon.rmq.subscribers;

import sarkkom.paragon.rmq.config.LocalConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.beans.factory.annotation.Autowired;

public class RejectAndDontRequeueRecoverer implements MessageRecoverer {
	protected Log logger = LogFactory.getLog(org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer.class);

	@Autowired
	RabbitAdmin rabbitAdmin;

	@Autowired
	RabbitTemplate amqpTemplate;

	@Override
	public void recover(Message message, Throwable cause) {
		if (logger.isWarnEnabled()) {
			logger.warn("Retries exhausted for message : " + (new String(message.getBody())), cause);
		}

		// came here because all retries done.

		StringBuilder msg = new StringBuilder();
		msg.append("Retry Policy Exhausted. Message moved to DLQ. ");
		msg.append("[msgId: ").append(message.getMessageProperties().getMessageId()).append("]");
		msg.append("[appId: ").append(message.getMessageProperties().getAppId()).append("]");
		msg.append("[correlationId: ").append(message.getMessageProperties().getCorrelationId()).append("]");
		msg.append("[cTag: ").append(message.getMessageProperties().getConsumerTag()).append("]");
		msg.append("[clusterId: ").append(message.getMessageProperties().getClusterId()).append("]");

		// move message to DLQ
		publishMessageDLQ(message);

		// throw exception to reject and don't requeue
		throw new ListenerExecutionFailedException(msg.toString(), new AmqpRejectAndDontRequeueException(cause));

	}

	public void publishMessageDLQ(Message message)  {
		try {
			logger.debug("moving message to Dead Letter Queue: " + LocalConfig.DLQ);

			// create dlq
			rabbitAdmin.declareQueue(new Queue(LocalConfig.DLQ, true, false, false, null));
			amqpTemplate.setMandatory(true);
			amqpTemplate.convertAndSend(LocalConfig.DLQ, message);
		} catch (Exception e) {
			// if any exception occurs, reject and don't requeue
			throw new AmqpRejectAndDontRequeueException(e.getMessage(),e);
		}
	}
}
