# schema_pymongo.py - Complete GraphQL schema using PyMongo directly
import graphene
from pymongo_workaround import PyMongoMovieService

# Initialize PyMongo service
movie_service = PyMongoMovieService()

class MovieType(graphene.ObjectType):
    """GraphQL Movie type using PyMongo data"""
    title = graphene.String()
    year = graphene.Int()
    rating = graphene.Float()
    genres = graphene.List(graphene.String)
    directors = graphene.List(graphene.String)
    runtime = graphene.Int()
    
    # Backward compatibility
    genre = graphene.List(graphene.String)
    
    @classmethod
    def from_dict(cls, movie_dict):
        """Create MovieType from dictionary, filtering out unwanted fields"""
        # Remove any fields that aren't part of our GraphQL type
        filtered_dict = {
            key: value for key, value in movie_dict.items() 
            if key in ['title', 'year', 'rating', 'genres', 'directors', 'runtime', 'genre']
        }
        return cls(**filtered_dict)
    
    def resolve_genre(self, info):
        """Backward compatibility"""
        return getattr(self, 'genres', []) or []

class Query(graphene.ObjectType):
    # Movie queries - keeping same names as original for compatibility
    movies_by_genre = graphene.List(MovieType, genre=graphene.String())
    movies_by_year = graphene.List(MovieType, year=graphene.Int())
    movies_by_rating = graphene.List(MovieType, min_rating=graphene.Float())
    all_movies_list = graphene.List(MovieType)
    
    # Connection-style queries for backward compatibility
    all_movies = graphene.Field(graphene.String)  # Placeholder - not implemented
    
    def resolve_movies_by_genre(self, info, genre):
        """Resolve movies by genre using PyMongo"""
        movies_data = movie_service.get_movies_by_genre(genre)
        return [MovieType.from_dict(movie) for movie in movies_data]
    
    def resolve_movies_by_year(self, info, year):
        """Resolve movies by year using PyMongo"""
        movies_data = movie_service.get_movies_by_year(year)
        return [MovieType.from_dict(movie) for movie in movies_data]
    
    def resolve_movies_by_rating(self, info, min_rating):
        """Resolve movies by rating using PyMongo"""
        movies_data = movie_service.get_movies_by_rating(min_rating)
        return [MovieType.from_dict(movie) for movie in movies_data]
    
    def resolve_all_movies_list(self, info):
        """Resolve all movies using PyMongo"""
        movies_data = movie_service.get_all_movies()
        return [MovieType.from_dict(movie) for movie in movies_data]
    
    def resolve_all_movies(self, info):
        """Placeholder for connection-style queries"""
        return "Use allMoviesList instead"

# Temporary schema using PyMongo
pymongo_schema = graphene.Schema(query=Query)

# Test the schema
def test_pymongo_schema():
    """Test the PyMongo-based GraphQL schema"""
    
    print("🧪 Testing PyMongo GraphQL Schema")
    print("=" * 35)
    
    # Test action movies query
    query = '''
    query {
        moviesByGenre(genre: "Action") {
            title
            year
            rating
            genres
            directors
        }
    }
    '''
    
    try:
        result = pymongo_schema.execute(query)
        
        if result.errors:
            print(f"❌ GraphQL errors: {result.errors}")
            return False
        
        if result.data and result.data['moviesByGenre']:
            movies = result.data['moviesByGenre']
            print(f"✅ SUCCESS! Found {len(movies)} action movies")
            
            print("📋 Sample action movies:")
            for movie in movies[:3]:
                print(f"   • {movie['title']} ({movie['year']}) - {movie['genres']}")
            
            return True
        else:
            print("❌ No movies returned")
            print(f"Debug - Full result: {result.data}")
            return False
            
    except Exception as e:
        print(f"❌ Schema test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_pymongo_schema()
    
    if success:
        print(f"\n🎉 PyMongo GraphQL schema works!")
        print(f"💡 You can temporarily use this instead of MongoEngine")
        print(f"🔧 Update your app.py to import pymongo_schema instead of schema")
    else:
        print(f"\n❌ PyMongo GraphQL schema failed")