# test_mongodb.py - Test your MongoDB connection and data
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import json

load_dotenv()

def test_mongodb_connection():
    """Test MongoDB Atlas connection and verify data"""
    
    mongodb_uri = os.getenv('MONGODB_URI')
    
    if not mongodb_uri:
        print("❌ MONGODB_URI not found in environment variables")
        print("Make sure you have a .env file with your MongoDB Atlas connection string")
        return False
    
    try:
        # Connect to MongoDB
        print("🔗 Connecting to MongoDB Atlas...")
        client = MongoClient(mongodb_uri)
        
        # Test connection
        client.admin.command('ping')
        print("✅ MongoDB connection successful!")
        
        # Get database
        db = client.imdb
        
        # Test collections exist
        collections = db.list_collection_names()
        print(f"📁 Available collections: {collections}")
        
        # Test movies collection
        movies_collection = db.movies
        movie_count = movies_collection.count_documents({})
        print(f"🎬 Total movies in database: {movie_count}")
        
        if movie_count > 0:
            # Show sample movie
            sample_movie = movies_collection.find_one()
            print("📄 Sample movie document:")
            # Remove _id for cleaner display
            if '_id' in sample_movie:
                del sample_movie['_id']
            print(json.dumps(sample_movie, indent=2))
            
            # Test action movies query
            action_movies = list(movies_collection.find(
                {'genre': {'$regex': 'Action', '$options': 'i'}},
                {'title': 1, 'year': 1, 'rating': 1, 'genre': 1}
            ).limit(5))
            
            print(f"\n🎯 Sample Action movies ({len(action_movies)} found):")
            for movie in action_movies:
                if '_id' in movie:
                    del movie['_id']
                print(f"  - {movie.get('title', 'Unknown')} ({movie.get('year', 'N/A')}) - Rating: {movie.get('rating', 'N/A')}")
        
        # Test genres collection
        genres_collection = db.genres
        genre_count = genres_collection.count_documents({})
        print(f"\n🏷️  Total genres in database: {genre_count}")
        
        if genre_count > 0:
            # Show sample genres
            sample_genres = list(genres_collection.find({}, {'name': 1, 'movie_count': 1}).limit(5))
            print("📋 Sample genres:")
            for genre in sample_genres:
                print(f"  - {genre.get('name', 'Unknown')}: {genre.get('movie_count', 0)} movies")
        
        return True
        
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("\n🔧 Troubleshooting tips:")
        print("1. Check your MongoDB Atlas connection string")
        print("2. Make sure your IP is whitelisted in MongoDB Atlas")
        print("3. Verify your username and password")
        print("4. Ensure the database name is 'imdb'")
        return False

def test_sample_queries():
    """Test sample MongoDB queries"""
    
    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        print("❌ Cannot test queries - MongoDB URI not found")
        return
    
    try:
        client = MongoClient(mongodb_uri)
        db = client.imdb
        movies = db.movies
        
        print("\n🧪 Testing sample MongoDB queries...")
        
        # Query 1: Find all movies
        print("\n1. Find all movies (limit 3):")
        all_movies = list(movies.find({}, {'title': 1, 'year': 1, 'rating': 1}).limit(3))
        for movie in all_movies:
            print(f"   {movie.get('title')} ({movie.get('year')}) - {movie.get('rating')}")
        
        # Query 2: Find action movies
        print("\n2. Find Action movies (limit 3):")
        action_movies = list(movies.find(
            {'genre': {'$regex': 'Action', '$options': 'i'}},
            {'title': 1, 'year': 1, 'rating': 1}
        ).limit(3))
        for movie in action_movies:
            print(f"   {movie.get('title')} ({movie.get('year')}) - {movie.get('rating')}")
        
        # Query 3: Count movies
        print("\n3. Count total movies:")
        total_count = movies.count_documents({})
        print(f"   Total: {total_count} movies")
        
        # Query 4: Average rating
        print("\n4. Calculate average rating:")
        avg_result = list(movies.aggregate([
            {'$group': {'_id': None, 'average_rating': {'$avg': '$rating'}}},
            {'$project': {'_id': 0, 'average_rating': 1}}
        ]))
        if avg_result:
            print(f"   Average rating: {avg_result[0]['average_rating']:.2f}")
        
        # Query 5: Movies by year
        print("\n5. Find movies from 2019 (limit 3):")
        movies_2019 = list(movies.find(
            {'year': 2019},
            {'title': 1, 'rating': 1}
        ).limit(3))
        for movie in movies_2019:
            print(f"   {movie.get('title')} - {movie.get('rating')}")
        
        print("\n✅ All queries executed successfully!")
        
    except Exception as e:
        print(f"❌ Query testing failed: {e}")

if __name__ == "__main__":
    print("🎬 IMDB MongoDB Connection Test")
    print("=" * 50)
    
    # Test connection and data
    if test_mongodb_connection():
        test_sample_queries()
    
    print("\n" + "=" * 50)
    print("Test completed!")