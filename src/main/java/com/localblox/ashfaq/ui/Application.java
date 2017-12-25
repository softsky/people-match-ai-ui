package com.localblox.ashfaq.ui;

import com.localblox.ashfaq.ui.storage.StorageProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(StorageProperties.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

/*    @Bean
    CommandLineRunner init(@Qualifier("HDFSStorageService") StorageService storageService) {
        return (args) -> {
            storageService.deleteAll();
            storageService.init();
        };
    }*/
}
