# data_import.py - Import IMDB CSV data to MongoDB Atlas
import pandas as pd
import pymongo
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import json

load_dotenv()

def clean_and_prepare_data(df):
    """Clean and prepare the IMDB data for MongoDB"""
    
    print("🧹 Cleaning and preparing data...")
    
    # Clean column names
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('-', '_')
    
    # Handle the specific IMDB columns
    column_mapping = {
        'rank': 'rank',
        'title': 'title',
        'genre': 'genre',
        'description': 'description',
        'director': 'directors',
        'actors': 'actors',
        'year': 'year',
        'runtime_minutes': 'runtime_minutes',
        'rating': 'rating',
        'votes': 'votes',
        'revenue_millions': 'revenue_millions'
    }
    
    # Rename columns to match our model
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # Convert data types
    if 'year' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    
    if 'runtime_minutes' in df.columns:
        df['runtime_minutes'] = pd.to_numeric(df['runtime_minutes'], errors='coerce').astype('Int64')
    
    if 'rating' in df.columns:
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    
    if 'votes' in df.columns:
        df['votes'] = pd.to_numeric(df['votes'], errors='coerce').astype('Int64')
    
    if 'revenue_millions' in df.columns:
        df['revenue_millions'] = pd.to_numeric(df['revenue_millions'], errors='coerce')
    
    # Process array fields (split by comma)
    array_fields = ['genre', 'directors', 'actors']
    for field in array_fields:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: [item.strip() for item in str(x).split(',')] if pd.notna(x) and str(x) != 'nan' else [])
    
    # Remove rows with missing essential data
    df = df.dropna(subset=['title'])
    
    print(f"✅ Data cleaned. {len(df)} movies ready for import.")
    return df

def import_imdb_data(csv_file_path='IMDB-Movie-Data.csv'):
    """Import IMDB CSV data to MongoDB Atlas"""
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"❌ CSV file not found: {csv_file_path}")
        print("Please make sure the IMDB-Movie-Data.csv file is in the current directory")
        return False
    
    # Connect to MongoDB
    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        print("❌ MONGODB_URI not found in environment variables")
        return False
    
    try:
        print("🔗 Connecting to MongoDB Atlas...")
        client = MongoClient(mongodb_uri)
        client.admin.command('ping')
        db = client.imdb
        print("✅ Connected to MongoDB Atlas")
        
        # Read CSV file
        print(f"📖 Reading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        print(f"📊 Loaded {len(df)} rows from CSV")
        print(f"📋 Columns: {list(df.columns)}")
        
        # Clean and prepare data
        df = clean_and_prepare_data(df)
        
        # Import movies
        print("🎬 Importing movies...")
        movies_collection = db.movies
        
        # Clear existing data (optional - comment out if you want to keep existing data)
        existing_count = movies_collection.count_documents({})
        if existing_count > 0:
            response = input(f"⚠️  Found {existing_count} existing movies. Replace them? (y/N): ")
            if response.lower() == 'y':
                movies_collection.drop()
                print("🗑️  Cleared existing movies")
            else:
                print("📝 Keeping existing movies, adding new ones...")
        
        # Convert DataFrame to records
        movies_data = df.to_dict('records')
        
        # Handle NaN values (convert to None for MongoDB)
        for movie in movies_data:
            for key, value in movie.items():
                if pd.isna(value):
                    movie[key] = None
        
        # Insert movies in batches
        batch_size = 100
        for i in range(0, len(movies_data), batch_size):
            batch = movies_data[i:i + batch_size]
            movies_collection.insert_many(batch)
            print(f"   Inserted batch {i//batch_size + 1}/{(len(movies_data)-1)//batch_size + 1}")
        
        print(f"✅ Imported {len(movies_data)} movies")
        
        # Create indexes for better performance
        print("🔍 Creating database indexes...")
        movies_collection.create_index([("title", 1)])
        movies_collection.create_index([("year", 1)])
        movies_collection.create_index([("rating", 1)])
        movies_collection.create_index([("genre", 1)])
        movies_collection.create_index([("directors", 1)])
        print("✅ Created database indexes")
        
        # Generate genres collection
        print("🏷️  Generating genres collection...")
        genres_collection = db.genres
        
        # Clear existing genres
        genres_collection.drop()
        
        # Extract unique genres
        all_genres = set()
        for movie in movies_data:
            if movie.get('genre'):
                all_genres.update(movie['genre'])
        
        # Create genre documents with movie counts
        genres_data = []
        for genre in all_genres:
            if genre and genre.strip():  # Skip empty genres
                genre_name = genre.strip()
                movie_count = sum(1 for movie in movies_data 
                                if movie.get('genre') and genre_name in movie['genre'])
                genres_data.append({
                    'name': genre_name,
                    'description': f'Movies in the {genre_name} genre',
                    'movie_count': movie_count
                })
        
        if genres_data:
            genres_collection.insert_many(genres_data)
            print(f"✅ Created {len(genres_data)} genres")
        
        # Create index for genres
        genres_collection.create_index([("name", 1)])
        
        # Print summary
        print("\n📊 Import Summary:")
        print(f"   Movies imported: {movies_collection.count_documents({})}")
        print(f"   Genres created: {genres_collection.count_documents({})}")
        print(f"   Database: imdb")
        print(f"   Collections: movies, genres")
        
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

def create_sample_data():
    """Create sample data if no CSV file is available"""
    
    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        print("❌ MONGODB_URI not found in environment variables")
        return False
    
    try:
        print("🔗 Connecting to MongoDB Atlas...")
        client = MongoClient(mongodb_uri)
        client.admin.command('ping')
        db = client.imdb
        
        # Sample movies data
        sample_movies = [
            {
                'title': 'Inception',
                'genre': ['Action', 'Sci-Fi', 'Thriller'],
                'description': 'A thief who steals corporate secrets through dream-sharing technology.',
                'directors': ['Christopher Nolan'],
                'actors': ['Leonardo DiCaprio', 'Marion Cotillard', 'Ellen Page'],
                'year': 2010,
                'runtime_minutes': 148,
                'rating': 8.8,
                'votes': 2000000,
                'revenue_millions': 829.9
            },
            {
                'title': 'The Dark Knight',
                'genre': ['Action', 'Crime', 'Drama'],
                'description': 'Batman faces the Joker in this epic superhero film.',
                'directors': ['Christopher Nolan'],
                'actors': ['Christian Bale', 'Heath Ledger', 'Aaron Eckhart'],
                'year': 2008,
                'runtime_minutes': 152,
                'rating': 9.0,
                'votes': 2500000,
                'revenue_millions': 1004.9
            },
            {
                'title': 'Interstellar',
                'genre': ['Adventure', 'Drama', 'Sci-Fi'],
                'description': 'A team of explorers travel through a wormhole in space.',
                'directors': ['Christopher Nolan'],
                'actors': ['Matthew McConaughey', 'Anne Hathaway', 'Jessica Chastain'],
                'year': 2014,
                'runtime_minutes': 169,
                'rating': 8.6,
                'votes': 1800000,
                'revenue_millions': 677.5
            },
            {
                'title': 'The Avengers',
                'genre': ['Action', 'Adventure', 'Sci-Fi'],
                'description': 'Earth\'s mightiest heroes must come together to stop an alien invasion.',
                'directors': ['Joss Whedon'],
                'actors': ['Robert Downey Jr.', 'Chris Evans', 'Scarlett Johansson'],
                'year': 2012,
                'runtime_minutes': 143,
                'rating': 8.0,
                'votes': 1300000,
                'revenue_millions': 1518.8
            },
            {
                'title': 'Pulp Fiction',
                'genre': ['Crime', 'Drama'],
                'description': 'The lives of two mob hitmen, a boxer, and others intertwine.',
                'directors': ['Quentin Tarantino'],
                'actors': ['John Travolta', 'Uma Thurman', 'Samuel L. Jackson'],
                'year': 1994,
                'runtime_minutes': 154,
                'rating': 8.9,
                'votes': 1900000,
                'revenue_millions': 214.2
            }
        ]
        
        # Insert sample movies
        movies_collection = db.movies
        movies_collection.drop()  # Clear existing
        movies_collection.insert_many(sample_movies)
        
        # Create sample genres
        genres_data = [
            {'name': 'Action', 'description': 'Action-packed movies', 'movie_count': 3},
            {'name': 'Sci-Fi', 'description': 'Science fiction movies', 'movie_count': 3},
            {'name': 'Drama', 'description': 'Dramatic movies', 'movie_count': 3},
            {'name': 'Crime', 'description': 'Crime movies', 'movie_count': 2},
            {'name': 'Adventure', 'description': 'Adventure movies', 'movie_count': 2},
            {'name': 'Thriller', 'description': 'Thriller movies', 'movie_count': 1}
        ]
        
        genres_collection = db.genres
        genres_collection.drop()  # Clear existing
        genres_collection.insert_many(genres_data)
        
        # Create indexes
        movies_collection.create_index([("title", 1)])
        movies_collection.create_index([("year", 1)])
        movies_collection.create_index([("rating", 1)])
        movies_collection.create_index([("genre", 1)])
        genres_collection.create_index([("name", 1)])
        
        print(f"✅ Created sample data: {len(sample_movies)} movies, {len(genres_data)} genres")
        return True
        
    except Exception as e:
        print(f"❌ Sample data creation failed: {e}")
        return False

if __name__ == "__main__":
    print("🎬 IMDB Data Import Tool")
    print("=" * 50)
    
    # Check for CSV file
    csv_files = ['IMDB-Movie-Data.csv', 'imdb_data.csv', 'movies.csv']
    csv_file = None
    
    for file_name in csv_files:
        if os.path.exists(file_name):
            csv_file = file_name
            break
    
    if csv_file:
        print(f"📁 Found CSV file: {csv_file}")
        success = import_imdb_data(csv_file)
    else:
        print("📁 No CSV file found. Available options:")
        print("1. Place your IMDB-Movie-Data.csv file in the current directory")
        print("2. Create sample data for testing")
        
        choice = input("\nCreate sample data? (y/N): ")
        if choice.lower() == 'y':
            success = create_sample_data()
        else:
            print("Please add your CSV file and run again.")
            success = False
    
    if success:
        print("\n✅ Data import completed successfully!")
        print("You can now test your application with:")
        print("  python test_mongodb.py")
        print("  python app.py")
    else:
        print("\n❌ Data import failed!")
    
    print("=" * 50)