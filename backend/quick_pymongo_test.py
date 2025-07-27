# quick_pymongo_test.py - Quick test of the PyMongo fixes
def test_pymongo_service():
    """Test the PyMongo service directly"""
    
    print("🧪 Testing PyMongo Service")
    print("=" * 30)
    
    try:
        from pymongo_workaround import PyMongoMovieService
        
        service = PyMongoMovieService()
        
        # Test action movies
        action_movies = service.get_movies_by_genre("Action")
        print(f"📊 Action movies returned: {len(action_movies)}")
        
        if action_movies:
            sample = action_movies[0]
            print(f"📋 Sample movie structure:")
            for key, value in sample.items():
                print(f"   {key}: {value} ({type(value)})")
            
            # Check if _id field is present
            if '_id' in sample:
                print(f"⚠️  _id field still present: {sample['_id']}")
                return False
            else:
                print(f"✅ _id field properly removed")
                return True
        else:
            print(f"❌ No action movies returned")
            return False
            
    except Exception as e:
        print(f"❌ PyMongo service test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_graphql_movietype():
    """Test the GraphQL MovieType creation"""
    
    print(f"\n🧪 Testing GraphQL MovieType")
    print("=" * 35)
    
    try:
        from schema_pymongo import MovieType
        
        # Sample movie data (like what PyMongo returns)
        sample_movie = {
            'title': 'Test Movie',
            'year': 2024,
            'rating': 8.5,
            'genres': ['Action', 'Drama'],
            'directors': ['Test Director'],
            'runtime': 120,
            'extra_field': 'should be ignored'  # This should be filtered out
        }
        
        # Test creating MovieType from dict
        movie_obj = MovieType.from_dict(sample_movie)
        print(f"✅ MovieType created successfully")
        print(f"   Title: {movie_obj.title}")
        print(f"   Year: {movie_obj.year}")
        print(f"   Genres: {movie_obj.genres}")
        
        return True
        
    except Exception as e:
        print(f"❌ MovieType test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_full_graphql_query():
    """Test full GraphQL query execution"""
    
    print(f"\n🧪 Testing Full GraphQL Query")
    print("=" * 35)
    
    try:
        from schema_pymongo import pymongo_schema
        
        # Test action movies query
        query = '''
        query {
            moviesByGenre(genre: "Action") {
                title
                year
                rating
                genres
            }
        }
        '''
        
        result = pymongo_schema.execute(query)
        
        if result.errors:
            print(f"❌ GraphQL errors:")
            for error in result.errors:
                print(f"   {error}")
            return False
        
        if result.data and result.data.get('moviesByGenre'):
            movies = result.data['moviesByGenre']
            print(f"✅ SUCCESS! GraphQL query returned {len(movies)} movies")
            
            if movies:
                sample = movies[0]
                print(f"📋 Sample movie from GraphQL:")
                print(f"   Title: {sample['title']}")
                print(f"   Year: {sample['year']}")
                print(f"   Genres: {sample['genres']}")
                
                # Check if this is real IMDB data
                if sample['title'] not in ['Test Movie', 'New Movie']:
                    print(f"🎉 SUCCESS! Getting real IMDB data: {sample['title']}")
                    return True
                else:
                    print(f"⚠️  Still getting test data")
                    return False
            else:
                print(f"❌ Movies list is empty")
                return False
        else:
            print(f"❌ No data returned")
            print(f"Debug - result.data: {result.data}")
            return False
            
    except Exception as e:
        print(f"❌ Full GraphQL test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔧 PyMongo Fix Verification")
    print("=" * 40)
    
    # Test 1: PyMongo service
    test1 = test_pymongo_service()
    
    if test1:
        # Test 2: MovieType creation
        test2 = test_graphql_movietype()
        
        if test2:
            # Test 3: Full GraphQL execution
            test3 = test_full_graphql_query()
            
            if test3:
                print(f"\n🎉 ALL TESTS PASSED!")
                print(f"✅ PyMongo service works correctly")
                print(f"✅ MovieType creation works")
                print(f"✅ GraphQL queries return real IMDB data")
                print(f"\n🚀 Ready to test with Flask!")
                print(f"   1. Update your app.py with the PyMongo version")
                print(f"   2. Start Flask: python app.py")
                print(f"   3. Test: python test_complete_fix.py")
            else:
                print(f"\n❌ GraphQL query test failed")
        else:
            print(f"\n❌ MovieType creation test failed")
    else:
        print(f"\n❌ PyMongo service test failed")