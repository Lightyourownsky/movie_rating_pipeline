-- Database Schema for Movie Data Pipeline
-- SQLite Database

-- Drop existing tables to ensure a clean setup
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS movies;

-- 1. Movies table: stores main movie and OMDb information
CREATE TABLE movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    year INTEGER,
    imdbId TEXT,
    director TEXT,
    plot TEXT,
    boxoffice TEXT,
    runtime TEXT,
    rated TEXT
);

-- 2. Genres table: stores unique genre names
CREATE TABLE genres (
    genreId INTEGER PRIMARY KEY AUTOINCREMENT,
    genreName TEXT UNIQUE NOT NULL
);

-- 3. Movie-Genres junction table: handles many-to-many relationship
CREATE TABLE movie_genres (
    movieId INTEGER,
    genreId INTEGER,
    PRIMARY KEY (movieId, genreId),
    FOREIGN KEY (movieId) REFERENCES movies(movieId),
    FOREIGN KEY (genreId) REFERENCES genres(genreId)
);

-- 4. Ratings table: stores user ratings for movies
CREATE TABLE ratings (
    ratingId INTEGER PRIMARY KEY AUTOINCREMENT,
    movieId INTEGER,
    userId INTEGER,
    rating REAL,
    timestamp INTEGER,
    FOREIGN KEY (movieId) REFERENCES movies(movieId)
);
