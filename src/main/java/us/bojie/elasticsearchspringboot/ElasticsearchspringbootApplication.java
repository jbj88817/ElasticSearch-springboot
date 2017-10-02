package us.bojie.elasticsearchspringboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class ElasticsearchspringbootApplication {

	public static void main(String[] args) {
		SpringApplication.run(ElasticsearchspringbootApplication.class, args);
	}

	@GetMapping("/")
	public String index() {
		return "index";
	}
}
