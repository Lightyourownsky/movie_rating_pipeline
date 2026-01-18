-- Database Schema for Movie Data Pipeline
-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS movies;

-- Movies table: stores main movie information
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

-- Genres table: stores unique genre names
CREATE TABLE genres (
    genreId INTEGER PRIMARY KEY AUTOINCREMENT,
    genreName TEXT UNIQUE NOT NULL
);

-- Movie_Genres junction table: links movies to their genres (many-to-many relationship)
CREATE TABLE movie_genres (
    movieId INTEGER,
    genreId INTEGER,
    PRIMARY KEY (movieId, genreId),
    FOREIGN KEY (movieId) REFERENCES movies(movieId),
    FOREIGN KEY (genreId) REFERENCES genres(genreId)
);

-- Ratings table: stores user ratings for movies
CREATE TABLE ratings (
    ratingId INTEGER PRIMARY KEY AUTOINCREMENT,
    movieId INTEGER,
    userId INTEGER,
    rating REAL,
    timestamp INTEGER,
    FOREIGN KEY (movieId) REFERENCES movies(movieId)
);

-- Creates indexes for better query performance
CREATE INDEX idx_ratings_movieId ON ratings(movieId);
CREATE INDEX idx_ratings_userId ON ratings(userId);
CREATE INDEX idx_movie_genres_movieId ON movie_genres(movieId);
CREATE INDEX idx_movie_genres_genreId ON movie_genres(genreId);
CREATE INDEX idx_movies_year ON movies(year);
