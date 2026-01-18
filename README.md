# Movie Data Pipeline

A complete ETL (Extract, Transform, Load) pipeline that ingests movie data from MovieLens CSV files, enriches it with data from the OMDb API, and loads it into a SQLite database for analysis.

## Overview

This project demonstrates a practical data engineering solution that:
- Extracts data from CSV files (MovieLens dataset)
- Enriches movie information using the OMDb REST API
- Transforms and cleans the data
- Loads it into a normalized relational database
- Enables analytical queries on the processed data

## Architecture

### Data Sources
1. **MovieLens Dataset**: CSV files containing movie titles, genres, and user ratings
2. **OMDb API**: External API providing detailed movie information (director, plot, box office, etc.)

### Database Schema

The database uses a **normalized relational design** with four tables:

- **movies**: Core movie information (title, year, IMDB ID, director, plot, etc.)
- **genres**: Unique genre names
- **movie_genres**: Junction table linking movies to genres (many-to-many)
- **ratings**: User ratings for movies

This design:
- Eliminates data redundancy (genres are normalized)
- Ensures data integrity through foreign key constraints
- Optimizes query performance with appropriate indexes

## Setup & Execution

### Prerequisites

- **Python 3.8+**
- **SQLite3** (comes pre-installed with Python)
- **pip** (Python package manager)
- **Internet connection** (for OMDb API calls)

### Step 1: Clone the Repository

```bash
git clone <your-repo-url>
cd movie-data-pipeline
```

### Step 2: Set Up Virtual Environment

Create and activate a virtual environment:

**On macOS/Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

**On Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

**Required packages:**
- pandas>=2.0.0
- requests>=2.28.0
- sqlalchemy>=2.0.0

### Step 4: Download MovieLens Dataset

1. Visit: https://grouplens.org/datasets/movielens/latest/
2. Download the **"small" dataset** (ml-latest-small.zip)
3. Extract the ZIP file
4. Create a `data/` directory in the project root
5. Copy `movies.csv` and `ratings.csv` to the `data/` directory

```bash
mkdir data
# Copy movies.csv and ratings.csv to data/
```

### Step 5: Get OMDb API Key

1. Visit: http://www.omdbapi.com/apikey.aspx
2. Sign up for a **free API key**
3. Open `etl.py` and update the `API_KEY` in the CONFIG dictionary (line 20):

```python
CONFIG = {
    'API_KEY': 'your_api_key_here',  # Replace with your actual API key
    'DATA_PATH': 'data',
    'DB_NAME': 'movie_database.db',
    'MOVIES_CSV': 'movies.csv',
    'RATINGS_CSV': 'ratings.csv',
    'API_DELAY': 1,
    'MAX_MOVIES': 100  # Set to None to process all movies
}
```


## Project Structure

```
movie-data-pipeline/
├── etl.py              # Main ETL pipeline script
├── schema.sql          # Database schema definition
├── queries.sql         # Analytical SQL queries
├── README.md           # Project documentation
├── requirements.txt    # Python dependencies
├── .gitignore          # Git ignore file
├── data/               # Directory for CSV files (not in repo)
│   ├── movies.csv
│   └── ratings.csv
└── movie_database.db   # SQLite database (created by script)
```

##  What Happens During Execution

When `python etl.py` file is run, the pipeline executes these steps:

### 1. **Database Setup** 
   - Creates SQLite database (`movie_database.db`)
   - Initializes tables (movies, genres, movie_genres, ratings)
   - Sets up foreign key relationships and indexes

### 2. **Data Extraction**
   - Reads `movies.csv` and `ratings.csv` from the data directory
   - Logs the number of movies and ratings loaded

### 3. **Data Transformation** 
   - Extracts release year from movie titles using regex
   - Cleans titles by removing year information
   - Prepares data for API enrichment

### 4. **API Enrichment** 
   - Fetches additional movie details from OMDb API
   - Retrieves: director, plot, box office, runtime, MPAA rating, IMDB ID
   - Implements rate limiting (1-second delay between requests)
   - Handles API errors gracefully (defaults to "N/A")

### 5. **Data Loading** 
   - Inserts enriched movies into the database
   - Populates the genres table with unique genres
   - Creates movie-genre relationships (many-to-many)
   - Loads user ratings for processed movies

### 6. **Verification** 
   - Confirms data integrity
   - Logs final record counts for all tables


*Note: The 1-second delay between API calls is necessary to respect OMDb's rate limits.*




## Troubleshooting

### Common Issues and Solutions

#### Issue 1: API Key Error
```
Error: Invalid API key
```
**Solution**: Verify your OMDb API key is correct in `etl.py`

#### Issue 2: CSV Files Not Found
```
FileNotFoundError: [Errno 2] No such file or directory: 'data/movies.csv'
```
**Solution**: 
- Ensure CSV files are in the `data/` directory
- Check the `DATA_PATH` configuration in `etl.py`

#### Issue 3: API Rate Limit Exceeded
```
Error: Too many requests
```
**Solution**: 
- Increase `API_DELAY` in CONFIG (e.g., to 2 seconds)
- Reduce `MAX_MOVIES` to process fewer movies

#### Issue 4: Database Already Exists
```
Table already exists error
```
**Solution**: The pipeline handles this automatically with `DROP TABLE IF EXISTS`. If issues persist:
```bash
rm movie_database.db
python etl.py
```

#### Issue 5: Import Errors
```
ModuleNotFoundError: No module named 'pandas'
```
**Solution**: Ensure virtual environment is activated and dependencies are installed:
```bash
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

##  Design Choices

### Why SQLite?
- Lightweight and serverless
- No additional setup required
- Perfect for development and small-to-medium datasets
- Easy to share (single file database)

### API Error Handling
- Graceful degradation: Movies not found in OMDb are stored with "N/A" values
- Rate limiting: 1-second delay between requests prevents API throttling
- Timeout handling: 10-second timeout per request
- Comprehensive logging for debugging

### Data Quality
- **Idempotency**: Script uses `INSERT OR REPLACE` and `INSERT OR IGNORE` to prevent duplicates
- **Normalization**: Genres are stored in a separate table to avoid redundancy
- **Validation**: Filters out invalid genre values like "(no genres listed)"
- **Type consistency**: Proper data types for all fields

### Performance Optimizations
- Indexes on foreign keys and frequently queried columns
- Batch commits instead of per-row commits
- Efficient pandas operations for data transformation

## Key Assumptions
1. Data Quality Assumptions
CSV Data:

Assumption: MovieLens CSV files are well-formed and consistent
Known Issue: Some movies may have irregular title formats (handled by regex with fallback)

API Data:

Assumption: Not all movies will match in OMDb (titles may differ, movies may not exist in their database)

2. Data Matching Assumptions
Title Matching:

Assumption: Cleaning titles (removing year) improves OMDb match rate
Assumption: Providing year as a parameter increases match accuracy
Limitation: Some movies may still not match due to:

Alternate titles (international releases)
Typos or formatting differences
Movies not in OMDb database

Example Mismatches:
CSV: "Star Wars: Episode IV - A New Hope (1977)"
OMDb: "Star Wars" or "Star Wars: A New Hope"


3. Performance Assumptions
Processing Limits:

Assumption: Default processing of 100 movies is reasonable for demonstration, 1-second API delay is acceptable trade-off for reliability

4. Database Assumptions
Idempotency:

Assumption: INSERT OR REPLACE and INSERT OR IGNORE provide idempotency, Re-running the script is safe (won't create duplicates)
Design: movieId serves as natural primary key from source data

## 🔧 Challenges & Solutions

### Challenge 1: API Rate Limiting
**Problem**: OMDb free tier has rate limits  
**Solution**: Implemented 1-second delay between requests and processing limit option

### Challenge 2: Title Matching
**Problem**: Movie titles in CSV don't always match OMDb exactly  
**Solution**: Extract and clean titles, use year parameter for better matching

### Challenge 3: Missing API Data
**Problem**: Not all movies are found in OMDb  
**Solution**: Default to "N/A" values, log warnings, continue processing

### Challenge 4: Genre Normalization
**Problem**: Genres stored as pipe-separated strings in CSV  
**Solution**: Created junction table with many-to-many relationship

## Challenge 5: API handling
**Problem**:Processing all 9,742 movies takes 3-4 hours (API rate limiting)
**Solution**: In production, Parallel API requests with proper rate limiting, Caching mechanism for previously fetched data,Incremental updates instead of full reprocessing (The solution isn't used in this)



##  Scaling Considerations

1. **Database & Infrastructure**
   Migration to PostgreSQL: To handle multiple users writing to the database simultaneously, I would migrate from SQLite to a more robust relational database like PostgreSQL.

   Containerization: I plan to use Docker to package the application. This would ensure that the environment (Python version, libraries, etc.) remains identical regardless of where the project is     deployed.

2. **Enhanced API Handling**
   Advanced Error Handling: I would implement the tenacity library to handle API failures gracefully using exponential backoff (retrying the request after a delay).

   Caching: To avoid hitting API rate limits and save costs, I would implement a caching layer (like Redis) to store and reuse movie data that rarely changes.

3. **Workflow & Data Quality**
   Orchestration with Airflow: Instead of running the script manually, I would use Apache Airflow to schedule the ETL process and track dependencies between tasks.

   Incremental Loading: Currently, the pipeline performs a full reload. A production version would implement a "Delta Load" to only process new movies or updated ratings since the last run.

4. **Monitoring & Validation**
   Data Quality Checks: I would add a validation step to the pipeline to ensure no "null" values or "zero" runtimes are loaded into the final database.

   Alerting: Integrating a simple logging and alerting system (like Discord or Slack webhooks) to notify me if the pipeline fails overnight.





##  Requirements File

Create `requirements.txt`:
```
pandas>=2.0.0
requests>=2.28.0
sqlalchemy>=2.0.0
```

##  Author

**Chathurya**  
