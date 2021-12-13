package com.github.obasekiosa.bookspacedataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.github.obasekiosa.bookspacedataloader.author.Author;
import com.github.obasekiosa.bookspacedataloader.author.AuthorRepository;
import com.github.obasekiosa.bookspacedataloader.connection.DataStaxAstraProperties;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookspaceDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BookspaceDataLoaderApplication.class, args);
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

	private void initAuthors() {

		Path path = Paths.get(authorDumpLocation);

		try (Stream<String> lines =  Files.lines(path)) {
			lines.forEach(line -> {
				// Read and parse the line
				String authorJsonStr = line.substring(line.indexOf("{"));
				JSONObject authorJson;
				try {
					authorJson = new JSONObject(authorJsonStr);
					// construct author object
					Author author = new Author();
					author.setName(authorJson.optString("name"));
					author.setPersonalName(authorJson.optString("personal_name"));
					author.setId(authorJson.optString("key").replace("/authors/", ""));
					// persist into database
					System.out.println("Saving author " + author.getName());
					authorRepository.save(author);
					

				} catch (JSONException e) {
					e.printStackTrace();
				}
				
				
			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void initWorks() {
		
	}

	@PostConstruct
	public void start() {
		// initAuthors();
		initWorks();
	}
}
