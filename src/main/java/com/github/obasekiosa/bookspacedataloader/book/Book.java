package com.github.obasekiosa.bookspacedataloader.book;

import java.time.LocalDate;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.CassandraType.Name;


@Table(value = "book_by_id")
public class Book {

    @Id
    @PrimaryKeyColumn(name = "book_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private String id;

    @Column("book_name")
    @CassandraType(type = Name.TEXT)
    private String name;

    @Column("cover_ids")
    @CassandraType(type = Name.LIST, typeArguments = Name.TEXT)
    private List<String> coverIds;

    @Column("author_names")
    @CassandraType(type = Name.LIST, typeArguments = Name.TEXT)
    private List<String> authorNames;

    @Column("author_ids")
    @CassandraType(type = Name.LIST, typeArguments = Name.TEXT)
    private List<String> authorIds;

    @Column("book_description")
    @CassandraType(type = Name.TEXT)
    private String description;

    @Column("published_date")
    @CassandraType(type = Name.DATE)
    private LocalDate publishDate;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getCoverIds() {
        return coverIds;
    }

    public void setCoverIds(List<String> coverIds) {
        this.coverIds = coverIds;
    }

    public List<String> getAuthorNames() {
        return authorNames;
    }

    public void setAuthorNames(List<String> authorNames) {
        this.authorNames = authorNames;
    }

    public List<String> getAuthorIds() {
        return authorIds;
    }

    public void setAuthorIds(List<String> authorIds) {
        this.authorIds = authorIds;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public LocalDate getPublishDate() {
        return publishDate;
    }

    public void setPublishDate(LocalDate publishDate) {
        this.publishDate = publishDate;
    }

    @Override
    public String toString() {
        return "Book [authorIds=" + authorIds + ", authorNames=" + authorNames + ", coverIds=" + coverIds
                + ", description=" + description + ", id=" + id + ", name=" + name + ", publishDate=" + publishDate
                + "]";
    }
    

    
    
}