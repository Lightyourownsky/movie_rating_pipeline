-- SQL Queries for Movie Data Analysis
-- Task 5: Answering Analytical Questions

-- 1. Which movie has the highest average rating?
SELECT m.title, AVG(r.rating) AS average_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title
ORDER BY average_rating DESC
LIMIT 1;

-- 2. What are the top 5 movie genres that have the highest average rating?
SELECT g.genre_name, AVG(r.rating) AS average_rating
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movieId = r.movieId
GROUP BY g.genre_name
ORDER BY average_rating DESC
LIMIT 5;

-- 3. Who is the director with the most movies in this dataset?
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director != ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. What is the average rating of movies released each year?
SELECT release_year, AVG(r.rating) AS average_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year DESC;
