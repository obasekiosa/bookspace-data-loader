package com.github.obasekiosa.bookspacedataloader.book;

import org.springframework.data.cassandra.repository.CassandraRepository;

@Repository
public interface BookRepository extends CassandraRepository<Book, String> {
    
}
