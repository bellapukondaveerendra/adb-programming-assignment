# raw_mongodb_check.py - Check raw MongoDB data without MongoEngine
import pymongo
import os
from dotenv import load_dotenv
import json

load_dotenv()

def check_raw_mongodb():
    """Check MongoDB data directly without MongoEngine"""
    
    print("🔍 Raw MongoDB Data Check")
    print("=" * 40)
    
    try:
        # Connect directly to MongoDB
        client = pymongo.MongoClient(os.getenv('MONGODB_URI'))
        db = client.imdb  # Your database name
        
        # Check collections
        collections = db.list_collection_names()
        print(f"📊 Available collections: {collections}")
        
        # Check movies collection
        movies_collection = db.movies
        total_movies = movies_collection.count_documents({})
        print(f"📊 Total movies in collection: {total_movies}")
        
        # Get sample movies
        print(f"\n📋 Sample Movies (Raw Data):")
        sample_movies = list(movies_collection.find().limit(5))
        
        for i, movie in enumerate(sample_movies):
            print(f"\n   Movie {i+1}:")
            print(f"      _id: {movie.get('_id')}")
            print(f"      title: {movie.get('title')}")
            print(f"      year: {movie.get('year')}")
            print(f"      rating: {movie.get('rating')}")
            
            # Check genre field - this is the key
            genre_field = movie.get('genre')
            print(f"      genre: {genre_field}")
            print(f"      genre type: {type(genre_field)}")
            
            # Check if there are other genre-related fields
            for key in movie.keys():
                if 'genre' in key.lower():
                    print(f"      {key}: {movie.get(key)}")
        
        # Search for action movies using different methods
        print(f"\n🔍 Searching for Action Movies (Raw Queries):")
        
        search_queries = [
            {"name": "Exact genre = 'Action'", "query": {"genre": "Action"}},
            {"name": "Genre contains 'Action'", "query": {"genre": {"$regex": "Action", "$options": "i"}}},
            {"name": "Genre array contains 'Action'", "query": {"genre": {"$in": ["Action"]}}},
            {"name": "Any field contains 'action'", "query": {"$text": {"$search": "action"}}},
            {"name": "Genre regex (case insensitive)", "query": {"genre": {"$regex": ".*action.*", "$options": "i"}}},
        ]
        
        for search in search_queries:
            try:
                count = movies_collection.count_documents(search["query"])
                print(f"   {search['name']}: {count} movies")
                
                if count > 0:
                    sample = movies_collection.find_one(search["query"])
                    print(f"      Sample: {sample.get('title')} - Genre: {sample.get('genre')}")
            except Exception as e:
                print(f"   {search['name']}: Error - {e}")
        
        # Check what genre values actually exist
        print(f"\n🏷️ Unique Genre Values in Database:")
        try:
            # Get distinct genre values
            distinct_genres = movies_collection.distinct("genre")
            print(f"   Found {len(distinct_genres)} distinct genre values")
            
            # Show first 10 unique genres
            for i, genre in enumerate(distinct_genres[:10]):
                print(f"   {i+1}. '{genre}' (type: {type(genre)})")
            
            # Look specifically for action-related genres
            action_like = [g for g in distinct_genres if 'action' in str(g).lower()]
            if action_like:
                print(f"\n   Action-like genres found:")
                for genre in action_like:
                    count = movies_collection.count_documents({"genre": genre})
                    print(f"      '{genre}': {count} movies")
        
        except Exception as e:
            print(f"   Error getting distinct genres: {e}")
        
        # Check genres collection
        print(f"\n🏷️ Genres Collection:")
        genres_collection = db.genres
        total_genres = genres_collection.count_documents({})
        print(f"   Total genres: {total_genres}")
        
        if total_genres > 0:
            sample_genres = list(genres_collection.find().limit(5))
            for genre in sample_genres:
                print(f"      • {genre.get('name')} (count: {genre.get('movie_count', 0)})")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Raw MongoDB check failed: {e}")
        return False

def check_test_vs_real_data():
    """Compare test data vs real data structure"""
    print(f"\n🔍 Test vs Real Data Comparison")
    print("=" * 40)
    
    try:
        client = pymongo.MongoClient(os.getenv('MONGODB_URI'))
        db = client.imdb
        movies_collection = db.movies
        
        # Find the test movie we know exists
        test_movie = movies_collection.find_one({"title": "Test Movie"})
        if test_movie:
            print(f"📋 Test Movie Structure:")
            for key, value in test_movie.items():
                print(f"   {key}: {value} (type: {type(value)})")
        
        # Find a real movie (not test movie)
        real_movie = movies_collection.find_one({
            "title": {"$ne": "Test Movie"}
        })
        if real_movie:
            print(f"\n📋 Real Movie Structure:")
            for key, value in real_movie.items():
                print(f"   {key}: {value} (type: {type(value)})")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Comparison failed: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Raw MongoDB Investigation")
    print("=" * 50)
    
    check_raw_mongodb()
    check_test_vs_real_data()
    
    print(f"\n💡 Next Steps:")
    print("1. Check the output above to see how genre field is stored")
    print("2. Look for field name differences (genre vs genres vs Genre)")
    print("3. Check data type differences (string vs array)")
    print("4. Update GraphQL resolver accordingly")