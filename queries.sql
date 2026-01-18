-- SQL Queries for Movie Data Analysis

-- 1. Which movie has the highest average rating?
-- Joining movies and ratings to find the top-rated title [cite: 30, 31, 52]
SELECT m.title, AVG(r.rating) AS average_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title
ORDER BY average_rating DESC
LIMIT 1;

-- 2. Top 5 movie genres with highest average rating [cite: 53]
-- Joins the junction table to handle movies with multiple genres [cite: 31, 45]
SELECT g.genre_name, AVG(r.rating) AS average_rating
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movieId = r.movieId
GROUP BY g.genre_name
ORDER BY average_rating DESC
LIMIT 5;

-- 3. Director with the most movies (Filtering out "N/A" from API gaps) [cite: 54, 81]
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL 
  AND director != 'N/A' 
  AND director != ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. Average rating of movies released each year [cite: 55]
SELECT release_year, AVG(r.rating) AS average_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year DESC;
