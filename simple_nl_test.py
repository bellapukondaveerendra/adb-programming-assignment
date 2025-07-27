# simple_nl_test.py - Simple test for natural language functionality
import requests
import json

def test_simple_flow():
    """Test the basic natural language to GraphQL flow"""
    
    print("🎬 Testing: 'Show me action movies'")
    print("=" * 40)
    
    # Step 1: Test natural language endpoint
    try:
        response = requests.post(
            "http://localhost:5000/natural-language-graphql",
            json={"input": "Show me action movies"},
            timeout=20
        )
        
        print(f"📡 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            print("\n📋 Response Structure:")
            for key in result.keys():
                print(f"   • {key}")
            
            if 'error' in result:
                print(f"\n❌ Error: {result['error']}")
                return False
            
            if 'generated_query' in result:
                print(f"\n📝 Generated GraphQL Query:")
                print(result['generated_query'])
            
            if 'parsing_method' in result:
                print(f"\n🔧 Parsing Method: {result['parsing_method']}")
            
            if 'data' in result:
                print(f"\n📊 GraphQL Execution Result:")
                data = result['data']
                if data and isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, list):
                            print(f"   {key}: {len(value)} items")
                        elif isinstance(value, dict) and 'edges' in value:
                            print(f"   {key}: {len(value['edges'])} edges")
                        else:
                            print(f"   {key}: {value}")
                else:
                    print("   No data or empty result")
            
            if 'errors' in result and result['errors']:
                print(f"\n⚠️ GraphQL Errors:")
                for error in result['errors']:
                    print(f"   - {error}")
            
            return True
        else:
            print(f"❌ HTTP Error: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to Flask server")
        print("💡 Make sure to run: python app.py")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_comparison():
    """Test the comparison endpoint"""
    print("\n🤖 Testing Comparison Endpoint")
    print("=" * 35)
    
    try:
        response = requests.post(
            "http://localhost:5000/natural-language-compare",
            json={"input": "Show me action movies"},
            timeout=25
        )
        
        if response.status_code == 200:
            result = response.json()
            
            print("📊 Comparison Results:")
            
            # GraphQL approach
            gql = result.get('graphql_approach', {})
            print(f"\n🔧 GraphQL Approach:")
            print(f"   Success: {gql.get('success', False)}")
            if gql.get('success'):
                print(f"   Method: {gql.get('parsing_method', 'N/A')}")
                if 'graphql_query' in gql:
                    print(f"   Query Generated: ✅")
                if 'execution_result' in gql:
                    print(f"   Executed: ✅")
            else:
                print(f"   Error: {gql.get('error', 'Unknown')}")
            
            # MongoDB approach
            mongo = result.get('mongodb_approach', {})
            print(f"\n🍃 MongoDB Approach:")
            print(f"   Success: {mongo.get('success', False)}")
            if mongo.get('success'):
                print(f"   Method: {mongo.get('parsing_method', 'N/A')}")
                if 'query_result' in mongo:
                    print(f"   Executed: ✅")
            else:
                print(f"   Error: {mongo.get('error', 'Unknown')}")
            
            return True
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🎯 Simple Natural Language Flow Test")
    print("=" * 50)
    
    # Test basic flow
    success1 = test_simple_flow()
    
    # Test comparison
    success2 = test_comparison()
    
    if success1 or success2:
        print("\n✅ At least one approach is working!")
    else:
        print("\n❌ Both approaches failed - check server and dependencies")
    
    print("\n💡 Troubleshooting tips:")
    print("   1. Make sure Flask server is running: python app.py")
    print("   2. Check Ollama is running: ollama serve") 
    print("   3. Verify model is available: ollama list")
    print("   4. Check MongoDB connection in .env file")
    print("   5. Verify there's data in MongoDB")