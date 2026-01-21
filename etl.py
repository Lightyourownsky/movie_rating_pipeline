"""
Movie Data Pipeline - ETL Script
Extracts data from MovieLens CSV files and OMDb API,
transforms and enriches the data, and loads it into SQLite database.
"""

import pandas as pd
import sqlite3
import requests
import time
import re
import logging
from typing import Optional, Dict
from pathlib import Path

# Configuration
CONFIG = {
    'API_KEY': '--------',  # Replace with your OMDb API key
    'DATA_PATH': 'data',  # Path to CSV files directory
    'DB_NAME': 'movie_database.db',
    'MOVIES_CSV': 'movies.csv',
    'RATINGS_CSV': 'ratings.csv',
    'API_DELAY': 1,  # Seconds between API calls to respect rate limits
    'MAX_MOVIES': 100  # Limit movies to process (set to None for all)
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MovieETL:
    """ETL Pipeline for Movie Data"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.conn = None
        self.cursor = None
        
    def setup_database(self):
        """Create database connection and schema"""
        logger.info(f"Setting up database: {self.config['DB_NAME']}")
        self.conn = sqlite3.connect(self.config['DB_NAME'])
        self.cursor = self.conn.cursor()
        
        # Execute schema from file or inline
        schema_file = Path('schema.sql')
        if schema_file.exists():
            with open(schema_file, 'r') as f:
                self.cursor.executescript(f.read())
        else:
            self._create_schema_inline()
        
        self.conn.commit()
        logger.info("Database schema created successfully")
    
    def _create_schema_inline(self):
        """Create schema if schema.sql not found"""
        schema = """
        DROP TABLE IF EXISTS ratings;
        DROP TABLE IF EXISTS movie_genres;
        DROP TABLE IF EXISTS genres;
        DROP TABLE IF EXISTS movies;
        
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
        
        CREATE TABLE genres (
            genreId INTEGER PRIMARY KEY AUTOINCREMENT,
            genreName TEXT UNIQUE NOT NULL
        );
        
        CREATE TABLE movie_genres (
            movieId INTEGER,
            genreId INTEGER,
            PRIMARY KEY (movieId, genreId),
            FOREIGN KEY (movieId) REFERENCES movies(movieId),
            FOREIGN KEY (genreId) REFERENCES genres(genreId)
        );
        
        CREATE TABLE ratings (
            ratingId INTEGER PRIMARY KEY AUTOINCREMENT,
            movieId INTEGER,
            userId INTEGER,
            rating REAL,
            timestamp INTEGER,
            FOREIGN KEY (movieId) REFERENCES movies(movieId)
        );
        """
        self.cursor.executescript(schema)
    
    def extract_csv_data(self) -> tuple:
        """Extract data from CSV files"""
        logger.info("Extracting data from CSV files...")
        
        movies_path = Path(self.config['DATA_PATH']) / self.config['MOVIES_CSV']
        ratings_path = Path(self.config['DATA_PATH']) / self.config['RATINGS_CSV']
        
        movies_df = pd.read_csv(movies_path)
        ratings_df = pd.read_csv(ratings_path)
        
        logger.info(f"Loaded {len(movies_df)} movies and {len(ratings_df)} ratings")
        return movies_df, ratings_df
    
    def extract_year(self, title: str) -> Optional[int]:
        """Extract year from movie title"""
        match = re.search(r'\((\d{4})\)', title)
        return int(match.group(1)) if match else None
    
    def clean_title(self, title: str) -> str:
        """Remove year from movie title"""
        return re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()
    
    def fetch_omdb_data(self, title: str, year: Optional[int]) -> Optional[Dict]:
        """Fetch additional movie data from OMDb API"""
        try:
            url = f"http://www.omdbapi.com/?apikey={self.config['API_KEY']}&t={title}"
            if year:
                url += f"&y={year}"
            
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get('Response') == 'True':
                return {
                    'director': data.get('Director', 'N/A'),
                    'plot': data.get('Plot', 'N/A'),
                    'boxoffice': data.get('BoxOffice', 'N/A'),
                    'runtime': data.get('Runtime', 'N/A'),
                    'rated': data.get('Rated', 'N/A'),
                    'imdbId': data.get('imdbID', 'N/A')
                }
            else:
                logger.warning(f"Movie not found in OMDb: {title}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching OMDb data for '{title}': {str(e)}")
            return None
    
    def transform_and_enrich(self, movies_df: pd.DataFrame) -> pd.DataFrame:
        """Transform and enrich movie data"""
        logger.info("Transforming and enriching movie data...")
        
        # Extract year and clean title
        movies_df['year'] = movies_df['title'].apply(self.extract_year)
        movies_df['clean_title'] = movies_df['title'].apply(self.clean_title)
        
        # Limit movies if configured
        if self.config['MAX_MOVIES']:
            movies_df = movies_df.head(self.config['MAX_MOVIES'])
            logger.info(f"Processing {len(movies_df)} movies (limited)")
        
        enriched_data = []
        total = len(movies_df)
        
        for idx, row in movies_df.iterrows():
            movie_data = {
                'movieId': row['movieId'],
                'title': row['title'],
                'year': row['year'],
                'genres': row['genres'],
                'director': 'N/A',
                'plot': 'N/A',
                'boxoffice': 'N/A',
                'runtime': 'N/A',
                'rated': 'N/A',
                'imdbId': 'N/A'
            }
            
            # Fetch from API
            api_data = self.fetch_omdb_data(row['clean_title'], row['year'])
            if api_data:
                movie_data.update(api_data)
            
            enriched_data.append(movie_data)
            
            # Progress logging
            if (idx + 1) % 10 == 0:
                logger.info(f"Processed {idx + 1}/{total} movies...")
            
            # Rate limiting
            time.sleep(self.config['API_DELAY'])
        
        enriched_df = pd.DataFrame(enriched_data)
        logger.info(f"Successfully enriched {len(enriched_df)} movies")
        return enriched_df
    
    def load_movies(self, movies_df: pd.DataFrame):
        """Load movies into database"""
        logger.info("Loading movies into database...")
        
        for _, row in movies_df.iterrows():
            self.cursor.execute("""
                INSERT OR REPLACE INTO movies 
                (movieId, title, year, imdbId, director, plot, boxoffice, runtime, rated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['movieId'], row['title'], row['year'], row['imdbId'],
                row['director'], row['plot'], row['boxoffice'], 
                row['runtime'], row['rated']
            ))
        
        self.conn.commit()
        logger.info(f"Loaded {len(movies_df)} movies")
    
    def load_genres(self, movies_df: pd.DataFrame):
        """Load genres and movie-genre relationships"""
        logger.info("Loading genres...")
        
        # Extract all unique genres
        all_genres = set()
        for genres_str in movies_df['genres']:
            if pd.notna(genres_str) and genres_str != '(no genres listed)':
                genres = genres_str.split('|')
                all_genres.update(genres)
        
        # Insert genres
        for genre in all_genres:
            self.cursor.execute(
                "INSERT OR IGNORE INTO genres (genreName) VALUES (?)", 
                (genre,)
            )
        
        self.conn.commit()
        
        # Create genre mapping
        self.cursor.execute("SELECT genreId, genreName FROM genres")
        genre_map = {name: id for id, name in self.cursor.fetchall()}
        
        # Insert movie-genre relationships
        for _, row in movies_df.iterrows():
            if pd.notna(row['genres']) and row['genres'] != '(no genres listed)':
                genres = row['genres'].split('|')
                for genre in genres:
                    if genre in genre_map:
                        self.cursor.execute("""
                            INSERT OR IGNORE INTO movie_genres (movieId, genreId)
                            VALUES (?, ?)
                        """, (row['movieId'], genre_map[genre]))
        
        self.conn.commit()
        logger.info(f"Loaded {len(all_genres)} genres")
    
    def load_ratings(self, ratings_df: pd.DataFrame, processed_movie_ids: set):
        """Load ratings for processed movies"""
        logger.info("Loading ratings...")
        
        # Filter ratings for processed movies only
        ratings_subset = ratings_df[ratings_df['movieId'].isin(processed_movie_ids)]
        
        for _, row in ratings_subset.iterrows():
            self.cursor.execute("""
                INSERT INTO ratings (movieId, userId, rating, timestamp)
                VALUES (?, ?, ?, ?)
            """, (row['movieId'], row['userId'], row['rating'], row['timestamp']))
        
        self.conn.commit()
        logger.info(f"Loaded {len(ratings_subset)} ratings")
    
    def verify_data(self):
        """Verify data was loaded correctly"""
        logger.info("Verifying data...")
        
        self.cursor.execute("SELECT COUNT(*) FROM movies")
        movie_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM genres")
        genre_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM ratings")
        rating_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM movie_genres")
        mg_count = self.cursor.fetchone()[0]
        
        logger.info(f"Movies: {movie_count}, Genres: {genre_count}, "
                   f"Ratings: {rating_count}, Movie-Genre links: {mg_count}")
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        try:
            logger.info("Starting ETL Pipeline...")
            
            # Setup
            self.setup_database()
            
            # Extract
            movies_df, ratings_df = self.extract_csv_data()
            
            # Transform
            enriched_movies = self.transform_and_enrich(movies_df)
            
            # Load
            self.load_movies(enriched_movies)
            self.load_genres(enriched_movies)
            
            processed_ids = set(enriched_movies['movieId'].values)
            self.load_ratings(ratings_df, processed_ids)
            
            # Verify
            self.verify_data()
            
            logger.info("ETL Pipeline completed successfully!")
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            raise
        
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed")


if __name__ == "__main__":
    etl = MovieETL(CONFIG)
    etl.run_pipeline()
