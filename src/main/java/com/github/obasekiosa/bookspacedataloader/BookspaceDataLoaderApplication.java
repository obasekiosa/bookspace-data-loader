package com.github.obasekiosa.bookspacedataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.github.obasekiosa.bookspacedataloader.author.Author;
import com.github.obasekiosa.bookspacedataloader.author.AuthorRepository;
import com.github.obasekiosa.bookspacedataloader.book.Book;
import com.github.obasekiosa.bookspacedataloader.book.BookRepository;
import com.github.obasekiosa.bookspacedataloader.connection.DataStaxAstraProperties;

import org.json.JSONArray;
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

	@Autowired
	BookRepository bookRepository;

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
				try {
					JSONObject authorJson = new JSONObject(authorJsonStr);
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
		Path path = Paths.get(worksDumpLocation);

		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				// Read and parse the line
				String bookJsonStr = line.substring(line.indexOf("{"));
				
				try {
					JSONObject bookJson = new JSONObject(bookJsonStr);
					// construct work object
					Book book = new Book();

					String bookId = bookJson.optString("key");
					if (bookId != null) {
						bookId = bookId.replace("/works/", "");
						book.setId(bookId);
					}
					
					book.setName(bookJson.optString("title"));

					JSONObject descriptionObj = bookJson.optJSONObject("description");
					if (descriptionObj != null){
						String description = descriptionObj.optString("value");
						if (description != null) {
							book.setDescription(descriptionObj.optString("value"));
						}
					}

					JSONObject dateObj = bookJson.optJSONObject("created");
					if (dateObj != null) {
						String dateStr = dateObj.optString("value");
						if (dateStr != null) {
							LocalDateTime date = LocalDateTime.parse(dateStr);
							book.setPublishDate(date.toLocalDate());
						}
					}

					JSONArray authorsArr = bookJson.optJSONArray("authors");
					if (authorsArr != null) {
						List<String> authorIds = new ArrayList<>();
						List<String> authorNames = new ArrayList<>();
						
						for (int i = 0; i < authorsArr.length(); i++) {
							JSONObject authorObj = authorsArr.getJSONObject(i);
							JSONObject authorIdObj = authorObj.optJSONObject("author");
							if (authorIdObj != null) {
								String authorId = authorIdObj.optString("key");
								if (authorId != null) {
									authorId = authorId.replace("/authors/", "");
									authorIds.add(authorId);

									Optional<Author> author = authorRepository.findById(authorId);
									if (author.isPresent()) {
										authorNames.add(author.get().getName());
									} else {
										authorNames.add("Unknown Author");
									}
								}

							}
						}
						book.setAuthorNames(authorNames);
						book.setAuthorIds(authorIds);
					}

					JSONArray coversArr = bookJson.optJSONArray("covers");
					if (coversArr != null) {
						
						List<String> coverIds = new ArrayList<>();
						for (int i = 0; i < coversArr.length(); i++) {
							coverIds.add(coversArr.getString(i));
						}
						book.setCoverIds(coverIds);
					}

					

					// persist work
					// System.out.println(book);
					System.out.println("Saving book " + book.getName());
					bookRepository.save(book);
					

				} catch (JSONException e) {
					e.printStackTrace();
				}

			});
		} catch ( IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {
		// initAuthors();
		initWorks();
	}
}
