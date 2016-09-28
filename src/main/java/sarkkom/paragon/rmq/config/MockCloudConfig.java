
package sarkkom.paragon.rmq.config;

import sarkkom.paragon.rmq.subscribers.ReceiverQ1;
import sarkkom.paragon.rmq.subscribers.RejectAndDontRequeueRecoverer;
import org.aopalliance.aop.Advice;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder.StatelessRetryInterceptorBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.retry.MissingMessageIdAdvice;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.policy.MapRetryContextCache;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Component
@Configuration
@Profile("mock")
public class MockCloudConfig implements ApplicationContextAware {
	@Autowired
	private ConnectionFactory connectionFactory;

	@Autowired
	AnnotationConfigApplicationContext context;

	@Autowired
	@Qualifier("rabbitTemplate")
	RabbitTemplate rabbitTemplate;

	final static String queueName = "queue1"; // "spring-boot";

	@Bean
	public RabbitTemplate rabbitTemplate() {
		RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);

		RetryTemplate retryTemplate = new RetryTemplate();
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(500);
		backOffPolicy.setMultiplier(10.0);
		backOffPolicy.setMaxInterval(10000);
		retryTemplate.setBackOffPolicy(backOffPolicy);

		rabbitTemplate.setRetryTemplate(retryTemplate);

		return rabbitTemplate;
	}

	public RabbitTemplate getRabbitTemplate() {
		return rabbitTemplate;
	}

	public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
		rabbitTemplate.setConnectionFactory(connectionFactory);
		this.rabbitTemplate = rabbitTemplate;
	}

	@Bean
	SimpleMessageListenerContainer container(
			MessageListenerAdapter listenerAdapter,
			RetryOperationsInterceptor retry,
			MissingMessageIdAdvice missingMessageIdAdvice) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setQueueNames(queueName);
		//	container.setMessageListener(listenerAdapter);
		// container.setMessageConverter(jsonMessageConverter());
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setAdviceChain(new Advice[]{missingMessageIdAdvice, retry});
		// we needto add missingMessageIdAdvice to advice chain to add message id in case it is null initially
		return container;
	}

	@Bean
	ReceiverQ1 receiver() {
		return new ReceiverQ1();
	}

	@Bean
	MessageListenerAdapter listenerAdapter(ReceiverQ1 receiverQ1) {
		return new MessageListenerAdapter(receiverQ1, "receiveMessage");
	}

	@Bean
	public RetryOperationsInterceptor retryAdvice() {
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(10000);
		;
		return StatelessRetryInterceptorBuilder.stateless().backOffPolicy(backOffPolicy).maxAttempts(3)
				.recoverer(new RejectAndDontRequeueRecoverer()).build();
	}

	@Bean
	public MissingMessageIdAdvice missingMessageIdAdvice() {
		MissingMessageIdAdvice missingMessageIdAdvice = new MissingMessageIdAdvice(new MapRetryContextCache());
		return missingMessageIdAdvice;
	}

	@Override
	public void setApplicationContext(ApplicationContext arg0) throws BeansException {
		for (String str : arg0.getBeanDefinitionNames()) {
			// System.out.println(str + "--------------->");
		}
	}
}
