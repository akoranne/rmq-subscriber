
package sarkkom.paragon.rmq.config;

import sarkkom.paragon.rmq.subscribers.ReceiverQ1;
import sarkkom.paragon.rmq.subscribers.RejectAndDontRequeueRecoverer;
import sarkkom.paragon.rmq.subscribers.ReceiverQ2;
import org.aopalliance.aop.Advice;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder.StatelessRetryInterceptorBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.MissingMessageIdAdvice;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.policy.MapRetryContextCache;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

@Component
@Configuration
@Profile("default")
public class LocalConfig implements RabbitListenerConfigurer, ApplicationContextAware {

	public final static String QUEUE1 = "queue1";
	public final static String QUEUE2 = "queue2";
	public final static String DLQ    = "dlq";

	private static ApplicationContext applicationContext = null;

	@Autowired
	AnnotationConfigApplicationContext context;

	@Autowired
	ConnectionFactory connectionFactory;

	@Bean
	public ConnectionFactory rabbitConnectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		connectionFactory.setChannelCacheSize(25);
		return connectionFactory;
	}


	@Bean
	public RabbitAdmin rabbitAdmin() {
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		return rabbitAdmin;
	}

	@Bean
	public RabbitTemplate amqpTemplate() {
		RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
		return rabbitTemplate;
	}


	@Override
	public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
		registrar.setMessageHandlerMethodFactory(myHandlerMethodFactory());
	}

	@Bean
	public DefaultMessageHandlerMethodFactory myHandlerMethodFactory() {
		DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
		factory.setMessageConverter(jackson2Converter());
		return factory;
	}

	@Bean
	public MappingJackson2MessageConverter jackson2Converter() {
		MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
		return converter;
	}

	@Bean
	public MessageConverter jsonMessageConverter() {
		return new Jackson2JsonMessageConverter();
	}

	@Override
	public void setApplicationContext(ApplicationContext arg0) throws BeansException {
		applicationContext = arg0;
//		for (String str : arg0.getBeanDefinitionNames()) {
//			 System.out.println("	---------------> " + str);
//		}
	}

	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	@Bean
	public ReceiverQ1 receiverQ1() {
		return new ReceiverQ1();
	}

	@Bean
	public SimpleMessageListenerContainer container_ReceiverQ1() {
		return getMessageListenerContainer(connectionFactory, QUEUE1, receiverQ1());
	}

	@Bean
	public ReceiverQ2 receiverQ2() {
		return new ReceiverQ2();
	}

	@Bean
	public SimpleMessageListenerContainer container_ReceiverQ2() {
		return getMessageListenerContainer(connectionFactory, QUEUE2, receiverQ2());
	}

	@Bean
	public MissingMessageIdAdvice missingMessageIdAdvice() {
		MissingMessageIdAdvice missingMessageIdAdvice = new MissingMessageIdAdvice(new MapRetryContextCache());
		return missingMessageIdAdvice;
	}

	@Bean
	RejectAndDontRequeueRecoverer requeueRecoverer() {
		return new RejectAndDontRequeueRecoverer();
	}

	@Bean
	public RetryOperationsInterceptor retryAdvice() {
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();

		backOffPolicy.setInitialInterval(1 * 100);
		backOffPolicy.setMultiplier(2.0);
		backOffPolicy.setMaxInterval(1 * 1000);

//		// for the first time, must wait for a second; each second has 1000 millis
//		backOffPolicy.setInitialInterval(1 * 1000);
//
//		backOffPolicy.setMultiplier(2.0);
//
//		// 40 minutes; each minute of 60 seconds; each second has 1000 millis
//		backOffPolicy.setMaxInterval(40 * 60 * 1000);

		// set max retries to 3
		// after that reject-and-dont-recoverer is invoked which throws a AmqpRejectAndDontRequeueException
		return StatelessRetryInterceptorBuilder.stateless().backOffPolicy(backOffPolicy).maxAttempts(3)
				.recoverer(requeueRecoverer()).build();
	}


	private SimpleMessageListenerContainer getMessageListenerContainer(
			ConnectionFactory factory,
			String queueName,
			MessageListener queueReceiver) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

		// attach this container to the active connection factory
		container.setConnectionFactory(factory);

		// by default SimpleAsyncTaskExecutor manages the thread instances.

		// the queue and handler.
		container.setQueueNames(queueName);
		container.setMessageListener(queueReceiver);
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);

		// the retry advice
		container.setAdviceChain(new Advice[]{
				new MissingMessageIdAdvice(new MapRetryContextCache()),
				retryAdvice()});

		// set the number of concurrent message listener(s).
		container.setConcurrentConsumers(3);

		// the max no of concurrent max listener(s)
		container.setMaxConcurrentConsumers(10);

		return container;
	}


	/**
	 * By default SimpleAsyncTaskExecutor manages the thread instances.
	 * If that is not enough, then create a custom task executor
	 * NOT USED
	 */
	@Bean
	public AsyncTaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(10);
		threadPoolTaskExecutor.setMaxPoolSize(200);
		threadPoolTaskExecutor.setQueueCapacity(0);
		threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

		return threadPoolTaskExecutor;
	}

}
