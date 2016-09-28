
package sarkkom.paragon.rmq;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application implements CommandLineRunner {
	//final static String queueName = "spring-boot";
	public static void main(String []args) throws Exception{
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// System.out.println(" running now... ");
	}
}
