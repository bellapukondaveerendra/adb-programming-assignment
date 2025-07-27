# pymongo_workaround.py - Bypass MongoEngine with direct PyMongo
import os
import pymongo
from dotenv import load_dotenv

load_dotenv()

class PyMongoMovieService:
    """Direct PyMongo service to bypass MongoEngine issues"""
    
    def __init__(self):
        self.client = pymongo.MongoClient(os.getenv('MONGODB_URI'))
        self.db = self.client.imdb
        self.movies_collection = self.db.movies
        self.genres_collection = self.db.genres
    
    def get_all_movies(self, limit=20):
        """Get all movies"""
        try:
            cursor = self.movies_collection.find({}, {
                'title': 1, 'year': 1, 'rating': 1, 
                'genres': 1, 'directors': 1, 'runtime': 1
            }).limit(limit)
            
            movies = []
            for doc in cursor:
                # Remove _id field to avoid GraphQL errors
                doc.pop('_id', None)
                # Add backward compatibility
                doc['genre'] = doc.get('genres', [])
                movies.append(doc)
            
            return movies
        except Exception as e:
            print(f"Error getting all movies: {e}")
            return []
    
    def get_movies_by_genre(self, genre):
        """Get movies by genre"""
        try:
            cursor = self.movies_collection.find(
                {'genres': {'$regex': genre, '$options': 'i'}},
                {
                    'title': 1, 'year': 1, 'rating': 1,
                    'genres': 1, 'directors': 1, 'runtime': 1
                }
            ).limit(20)
            
            movies = []
            for doc in cursor:
                # Remove _id field to avoid GraphQL errors
                doc.pop('_id', None)
                doc['genre'] = doc.get('genres', [])
                movies.append(doc)
            
            return movies
        except Exception as e:
            print(f"Error getting movies by genre: {e}")
            return []
    
    def get_movies_by_year(self, year):
        """Get movies by year"""
        try:
            cursor = self.movies_collection.find(
                {'year': year},
                {
                    'title': 1, 'year': 1, 'rating': 1,
                    'genres': 1, 'directors': 1
                }
            ).limit(20)
            
            movies = []
            for doc in cursor:
                # Remove _id field to avoid GraphQL errors
                doc.pop('_id', None)
                doc['genre'] = doc.get('genres', [])
                movies.append(doc)
            
            return movies
        except Exception as e:
            print(f"Error getting movies by year: {e}")
            return []
    
    def get_movies_by_rating(self, min_rating):
        """Get movies by minimum rating"""
        try:
            cursor = self.movies_collection.find(
                {'rating': {'$gte': min_rating}},
                {
                    'title': 1, 'year': 1, 'rating': 1,
                    'genres': 1, 'directors': 1
                }
            ).sort('rating', -1).limit(20)
            
            movies = []
            for doc in cursor:
                # Remove _id field to avoid GraphQL errors
                doc.pop('_id', None)
                doc['genre'] = doc.get('genres', [])
                movies.append(doc)
            
            return movies
        except Exception as e:
            print(f"Error getting movies by rating: {e}")
            return []
    
    def count_movies(self):
        """Count total movies"""
        try:
            return self.movies_collection.count_documents({})
        except Exception as e:
            print(f"Error counting movies: {e}")
            return 0

# Test the workaround
def test_pymongo_workaround():
    """Test the PyMongo workaround"""
    
    print("🧪 Testing PyMongo Workaround")
    print("=" * 35)
    
    try:
        service = PyMongoMovieService()
        
        # Test count
        total = service.count_movies()
        print(f"📊 Total movies via PyMongo: {total}")
        
        if total > 1:
            print("✅ SUCCESS! PyMongo workaround can see all movies")
            
            # Test action movies
            action_movies = service.get_movies_by_genre("Action")
            print(f"🎬 Action movies found: {len(action_movies)}")
            
            if action_movies:
                print("📋 Sample action movies:")
                for movie in action_movies[:3]:
                    print(f"   • {movie['title']} ({movie['year']}) - {movie['genres']}")
                
                return True
            else:
                print("⚠️  No action movies found")
                # Show sample genres
                sample_movies = service.get_all_movies(10)
                sample_genres = set()
                for movie in sample_movies:
                    if movie.get('genres'):
                        sample_genres.update(movie['genres'])
                print(f"Available genres: {sorted(list(sample_genres))}")
                return False
        else:
            print("❌ PyMongo workaround still only sees 1 movie")
            return False
            
    except Exception as e:
        print(f"❌ PyMongo workaround failed: {e}")
        return False

if __name__ == "__main__":
    success = test_pymongo_workaround()
    
    if success:
        print(f"\n🎉 PyMongo workaround works!")
        print(f"💡 This confirms the issue is MongoEngine-specific")
        print(f"🔧 We can use this as a temporary fix for your GraphQL resolvers")
    else:
        print(f"\n❌ Even PyMongo workaround failed")
        print(f"🔍 This suggests a deeper database/connection issue")