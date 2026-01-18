-- Analytical Queries for Movie Database

-- Query 1: Which movie has the highest average rating?
-- Note: Filtering for movies with at least 5 ratings to ensure statistical significance
SELECT 
    m.title, 
    AVG(r.rating) as avg_rating, 
    COUNT(r.rating) as num_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.title
HAVING COUNT(r.rating) >= 5
ORDER BY avg_rating DESC
LIMIT 1;

-- Query 2: What are the top 5 movie genres that have the highest average rating?
SELECT 
    g.genreName, 
    AVG(r.rating) as avg_rating, 
    COUNT(r.rating) as num_ratings
FROM genres g
JOIN movie_genres mg ON g.genreId = mg.genreId
JOIN ratings r ON mg.movieId = r.movieId
GROUP BY g.genreId, g.genreName
ORDER BY avg_rating DESC
LIMIT 5;

-- Query 3: Who is the director with the most movies in this dataset?
SELECT 
    director, 
    COUNT(*) as movie_count
FROM movies
WHERE director != 'N/A' 
    AND director IS NOT NULL 
    AND director != ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- Query 4: What is the average rating of movies released each year?
SELECT 
    m.year, 
    AVG(r.rating) as avg_rating, 
    COUNT(DISTINCT m.movieId) as num_movies,
    COUNT(r.rating) as total_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year DESC;

-- BONUS QUERIES

-- Top 10 highest rated movies (with at least 10 ratings)
SELECT 
    m.title,
    m.year,
    AVG(r.rating) as avg_rating,
    COUNT(r.rating) as num_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.title, m.year
HAVING COUNT(r.rating) >= 10
ORDER BY avg_rating DESC
LIMIT 10;

-- Movies by decade with average ratings
SELECT 
    CAST((m.year / 10) * 10 AS INTEGER) as decade,
    COUNT(DISTINCT m.movieId) as num_movies,
    AVG(r.rating) as avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE m.year IS NOT NULL
GROUP BY decade
ORDER BY decade DESC;
