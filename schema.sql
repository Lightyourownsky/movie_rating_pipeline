--Final Schema for Movie Data Pipeline Assignment
-- Task 3

-- Drop tables in reverse order of dependencies to avoid foreign key errors
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS movies;

-- 1. Movies Table: Stores core data from CSV and OMDb API [cite: 37, 44]
CREATE TABLE movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    release_year INTEGER,
    director TEXT,
    plot TEXT,
    box_office TEXT,
    runtime TEXT
);

-- 2. Genres Table: Stores unique genre categories 
CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name TEXT UNIQUE NOT NULL
);

-- 3. Movie_Genres: Link table to handle the | separated genres (Feature Engineering) 
CREATE TABLE movie_genres (
    movieId INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movieId, genre_id),
    FOREIGN KEY (movieId) REFERENCES movies(movieId),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

-- 4. Ratings Table: Stores user rating data from ratings.csv
CREATE TABLE ratings (
    rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
    userId INTEGER,
    movieId INTEGER,
    rating REAL,
    timestamp INTEGER,
    FOREIGN KEY (movieId) REFERENCES movies(movieId)
);
