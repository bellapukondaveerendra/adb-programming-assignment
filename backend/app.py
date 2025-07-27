# # app.py - Enhanced version with MongoDB query support
# import os
# from flask import Flask, request, jsonify
# from flask_cors import CORS
# from graphene import Schema
# from mongoengine import connect
# from schema import schema
# from llm_processor import LLMProcessor
# import json
# from dotenv import load_dotenv

# load_dotenv()

# app = Flask(__name__)
# CORS(app)

# # MongoDB connection with error handling
# try:
#     mongodb_uri = os.getenv('MONGODB_URI')
#     if mongodb_uri:
#         connect(host=mongodb_uri)
#         print("✅ Connected to MongoDB Atlas")
#     else:
#         print("⚠️  MongoDB URI not found in environment variables")
#         print("Add MONGODB_URI to your .env file")
# except Exception as e:
#     print(f"❌ MongoDB connection failed: {e}")

# # Initialize LLM processor
# try:
#     llm_processor = LLMProcessor(model_name=os.getenv('OLLAMA_MODEL', 'llama2'))
#     print("✅ LLM Processor initialized")
# except Exception as e:
#     print(f"⚠️  LLM Processor initialization warning: {e}")
#     llm_processor = LLMProcessor()  # Will use fallback mode

# @app.route('/graphql', methods=['POST'])
# def graphql_endpoint():
#     """Standard GraphQL endpoint"""
#     data = request.get_json()
    
#     try:
#         result = schema.execute(
#             data.get('query'),
#             variables=data.get('variables'),
#             context={'request': request}
#         )
        
#         response = {
#             'data': result.data,
#             'errors': [str(error) for error in result.errors] if result.errors else None
#         }
        
#         return jsonify(response)
    
#     except Exception as e:
#         return jsonify({'errors': [str(e)]})

# @app.route('/natural-language-graphql', methods=['POST'])
# def natural_language_graphql_endpoint():
#     """Process natural language input and convert to GraphQL"""
#     data = request.get_json()
#     user_input = data.get('input', '')
    
#     if not user_input:
#         return jsonify({'error': 'No input provided'})
    
#     # Convert natural language to GraphQL
#     llm_result = llm_processor.natural_language_to_graphql(user_input)
    
#     if not llm_result['success']:
#         return jsonify({'error': llm_result['error']})
    
#     # Execute the generated GraphQL query
#     try:
#         result = schema.execute(llm_result['graphql_query'])
        
#         response = {
#             'approach': 'GraphQL',
#             'original_input': user_input,
#             'generated_query': llm_result['graphql_query'],
#             'data': result.data,
#             'errors': [str(error) for error in result.errors] if result.errors else None
#         }
        
#         # Add note if available
#         if 'note' in llm_result:
#             response['note'] = llm_result['note']
        
#         return jsonify(response)
    
#     except Exception as e:
#         return jsonify({
#             'approach': 'GraphQL',
#             'original_input': user_input,
#             'generated_query': llm_result['graphql_query'],
#             'error': f'GraphQL execution error: {str(e)}'
#         })

# @app.route('/natural-language-mongodb', methods=['POST'])
# def natural_language_mongodb_endpoint():
#     """Process natural language input and convert to MongoDB query"""
#     data = request.get_json()
#     user_input = data.get('input', '')
    
#     if not user_input:
#         return jsonify({'error': 'No input provided'})
    
#     # Convert natural language to MongoDB query and execute
#     mongodb_result = llm_processor.natural_language_to_mongodb(user_input)
#     print(f"mongodb_result",mongodb_result)
#     if not mongodb_result['success']:
#         return jsonify({'error': mongodb_result['error']})
    
#     response = {
#         'approach': 'MongoDB',
#         'original_input': user_input,
#         'mongodb_query': mongodb_result['mongodb_query'],
#         'query_result': mongodb_result['query_result']
#     }
    
#     return jsonify(response)

# @app.route('/natural-language', methods=['POST'])
# def natural_language_endpoint():
#     """Process natural language with both GraphQL and MongoDB (default to GraphQL for compatibility)"""
#     data = request.get_json()
#     user_input = data.get('input', '')
#     approach = data.get('approach', 'graphql').lower()  # 'graphql', 'mongodb', or 'both'
    
#     if not user_input:
#         return jsonify({'error': 'No input provided'})
    
#     if approach == 'mongodb':
#         return natural_language_mongodb_endpoint()
#     elif approach == 'both':
#         return natural_language_compare_endpoint()
#     else:
#         return natural_language_graphql_endpoint()

# @app.route('/natural-language-compare', methods=['POST'])
# def natural_language_compare_endpoint():
#     """Compare GraphQL vs MongoDB approaches for the same natural language input"""
#     data = request.get_json()
#     user_input = data.get('input', '')
    
#     if not user_input:
#         return jsonify({'error': 'No input provided'})
    
#     # Get comparison results
#     comparison_result = llm_processor.compare_graphql_vs_mongodb(user_input)
    
#     # Execute GraphQL query if successful
#     if comparison_result['graphql_approach']['success']:
#         try:
#             graphql_result = schema.execute(comparison_result['graphql_approach']['graphql_query'])
#             comparison_result['graphql_approach']['execution_result'] = {
#                 'data': graphql_result.data,
#                 'errors': [str(error) for error in graphql_result.errors] if graphql_result.errors else None
#             }
#         except Exception as e:
#             comparison_result['graphql_approach']['execution_error'] = str(e)
    
#     return jsonify(comparison_result)

# @app.route('/mongodb-query', methods=['POST'])
# def direct_mongodb_query():
#     """Execute direct MongoDB query"""
#     data = request.get_json()
    
#     required_fields = ['collection', 'operation']
#     if not all(field in data for field in required_fields):
#         return jsonify({'error': 'Missing required fields: collection, operation'})
    
#     try:
#         # Execute the MongoDB query using the processor
#         result = llm_processor._execute_mongodb_query(data)
        
#         return jsonify({
#             'query': data,
#             'result': result
#         })
    
#     except Exception as e:
#         return jsonify({'error': str(e)})

# @app.route('/health', methods=['GET'])
# def health_check():
#     return jsonify({
#         'status': 'healthy',
#         'endpoints': {
#             'graphql': '/graphql',
#             'natural_language_graphql': '/natural-language-graphql',
#             'natural_language_mongodb': '/natural-language-mongodb',
#             'natural_language_compare': '/natural-language-compare',
#             'direct_mongodb': '/mongodb-query'
#         },
#         'llm_processor': type(llm_processor).__name__,
#         'mongodb_connected': llm_processor.db is not None
#     })

# @app.route('/sample-queries', methods=['GET'])
# def sample_queries():
#     """Provide sample queries for testing both approaches"""
#     samples = {
#         'natural_language_examples': [
#             'Show me all action movies',
#             'Find movies from 2019',
#             'Get movies with rating above 8.0',
#             'Show all genres',
#             'Count how many movies we have',
#             'What is the average movie rating?',
#             'Show movies directed by Christopher Nolan',
#             'Add a new movie called "Test Movie"'
#         ],
#         'graphql_examples': [
#             {
#                 'name': 'All Movies',
#                 'query': '''
#                 query {
#                   allMovies {
#                     edges {
#                       node {
#                         title
#                         year
#                         rating
#                         genre
#                       }
#                     }
#                   }
#                 }
#                 '''
#             },
#             {
#                 'name': 'Movies by Genre',
#                 'query': '''
#                 query {
#                   moviesByGenre(genre: "Action") {
#                     title
#                     year
#                     rating
#                   }
#                 }
#                 '''
#             }
#         ],
#         'mongodb_examples': [
#             {
#                 'name': 'Find All Movies',
#                 'query': {
#                     'collection': 'movies',
#                     'operation': 'find',
#                     'filter': {},
#                     'projection': {'title': 1, 'year': 1, 'rating': 1}
#                 }
#             },
#             {
#                 'name': 'Find Action Movies',
#                 'query': {
#                     'collection': 'movies',
#                     'operation': 'find',
#                     'filter': {'genre': {'$regex': 'Action', '$options': 'i'}},
#                     'projection': {'title': 1, 'year': 1, 'rating': 1, 'genre': 1}
#                 }
#             },
#             {
#                 'name': 'Count Total Movies',
#                 'query': {
#                     'collection': 'movies',
#                     'operation': 'count_documents',
#                     'filter': {}
#                 }
#             },
#             {
#                 'name': 'Average Rating',
#                 'query': {
#                     'collection': 'movies',
#                     'operation': 'aggregate',
#                     'pipeline': [
#                         {'$group': {'_id': None, 'average_rating': {'$avg': '$rating'}}},
#                         {'$project': {'_id': 0, 'average_rating': 1}}
#                     ]
#                 }
#             }
#         ]
#     }
#     return jsonify(samples)

# @app.route('/demo', methods=['GET'])
# def demo_page():
#     """Simple demo page showing both approaches"""
#     html = '''
#     <!DOCTYPE html>
#     <html>
#     <head>
#         <title>GraphQL vs MongoDB Demo</title>
#         <style>
#             body { font-family: Arial, sans-serif; margin: 40px; }
#             .container { max-width: 1200px; margin: 0 auto; }
#             .demo-section { margin: 20px 0; padding: 20px; border: 1px solid #ddd; }
#             textarea { width: 100%; height: 100px; }
#             button { padding: 10px 20px; margin: 10px 0; }
#             .result { background: #f5f5f5; padding: 10px; margin: 10px 0; }
#         </style>
#     </head>
#     <body>
#         <div class="container">
#             <h1>🎬 IMDB Database - GraphQL vs MongoDB</h1>
            
#             <div class="demo-section">
#                 <h2>Natural Language Query</h2>
#                 <textarea id="userInput" placeholder="Enter your query (e.g., 'Show me all action movies')">Show me all action movies</textarea>
#                 <br>
#                 <button onclick="testGraphQL()">Test with GraphQL</button>
#                 <button onclick="testMongoDB()">Test with MongoDB</button>
#                 <button onclick="testBoth()">Compare Both</button>
                
#                 <div id="results" class="result"></div>
#             </div>
#         </div>
        
#         <script>
#             async function testGraphQL() {
#                 const input = document.getElementById('userInput').value;
#                 const response = await fetch('/natural-language-graphql', {
#                     method: 'POST',
#                     headers: {'Content-Type': 'application/json'},
#                     body: JSON.stringify({input: input})
#                 });
#                 const result = await response.json();
#                 document.getElementById('results').innerHTML = '<h3>GraphQL Result:</h3><pre>' + JSON.stringify(result, null, 2) + '</pre>';
#             }
            
#             async function testMongoDB() {
#                 const input = document.getElementById('userInput').value;
#                 const response = await fetch('/natural-language-mongodb', {
#                     method: 'POST',
#                     headers: {'Content-Type': 'application/json'},
#                     body: JSON.stringify({input: input})
#                 });
#                 const result = await response.json();
#                 document.getElementById('results').innerHTML = '<h3>MongoDB Result:</h3><pre>' + JSON.stringify(result, null, 2) + '</pre>';
#             }
            
#             async function testBoth() {
#                 const input = document.getElementById('userInput').value;
#                 const response = await fetch('/natural-language-compare', {
#                     method: 'POST',
#                     headers: {'Content-Type': 'application/json'},
#                     body: JSON.stringify({input: input})
#                 });
#                 const result = await response.json();
#                 document.getElementById('results').innerHTML = '<h3>Comparison Result:</h3><pre>' + JSON.stringify(result, null, 2) + '</pre>';
#             }
#         </script>
#     </body>
#     </html>
#     '''
#     return html

# if __name__ == '__main__':
#     port = int(os.getenv('FLASK_PORT', 5000))
#     print(f"🚀 Starting Flask server on port {port}")
#     print(f"📊 GraphQL endpoint: http://localhost:{port}/graphql")
#     print(f"🤖 Natural Language GraphQL: http://localhost:{port}/natural-language-graphql")
#     print(f"🍃 Natural Language MongoDB: http://localhost:{port}/natural-language-mongodb")
#     print(f"⚖️  Compare both approaches: http://localhost:{port}/natural-language-compare")
#     print(f"🔧 Direct MongoDB queries: http://localhost:{port}/mongodb-query")
#     print(f"🏥 Health check: http://localhost:{port}/health")
#     print(f"📋 Sample queries: http://localhost:{port}/sample-queries")
#     print(f"🎭 Demo page: http://localhost:{port}/demo")
    
#     app.run(debug=True, host='0.0.0.0', port=port)



# app.py - FIXED with PyMongo workaround
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from graphene import Schema
# REMOVED: from mongoengine import connect, disconnect
# REMOVED: from schema import schema
from schema_pymongo import pymongo_schema as schema  # Use PyMongo schema
from llm_processor import LLMProcessor
import json
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

# REMOVED: MongoDB MongoEngine connection (PyMongo handles this directly)
print("✅ Using PyMongo direct connection (bypassing MongoEngine)")

# Initialize LLM processor
try:
    llm_processor = LLMProcessor(model_name=os.getenv('OLLAMA_MODEL', 'llama2'))
    print("✅ LLM Processor initialized")
except Exception as e:
    print(f"⚠️  LLM Processor initialization warning: {e}")
    llm_processor = LLMProcessor()  # Will use fallback mode

@app.route('/graphql', methods=['POST'])
def graphql_endpoint():
    """Standard GraphQL endpoint using PyMongo schema"""
    data = request.get_json()
    
    try:
        result = schema.execute(
            data.get('query'),
            variables=data.get('variables'),
            context={'request': request}
        )
        
        response = {
            'data': result.data,
            'errors': [str(error) for error in result.errors] if result.errors else None
        }
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({'errors': [str(e)]})

@app.route('/natural-language-graphql', methods=['POST'])
def natural_language_graphql_endpoint():
    """Process natural language input and convert to GraphQL"""
    data = request.get_json()
    user_input = data.get('input', '')
    
    if not user_input:
        return jsonify({'error': 'No input provided'})
    
    # Convert natural language to GraphQL
    llm_result = llm_processor.natural_language_to_graphql(user_input)
    
    if not llm_result['success']:
        return jsonify({'error': llm_result['error']})
    
    # Execute the generated GraphQL query using PyMongo schema
    try:
        result = schema.execute(llm_result['graphql_query'])
        
        response = {
            'approach': 'GraphQL',
            'original_input': user_input,
            'generated_query': llm_result['graphql_query'],
            'data': result.data,
            'errors': [str(error) for error in result.errors] if result.errors else None
        }
        
        # Add note if available
        if 'note' in llm_result:
            response['note'] = llm_result['note']
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({
            'approach': 'GraphQL',
            'original_input': user_input,
            'generated_query': llm_result['graphql_query'],
            'error': f'GraphQL execution error: {str(e)}'
        })

# Keep all your other endpoints exactly the same
@app.route('/natural-language-mongodb', methods=['POST'])
def natural_language_mongodb_endpoint():
    """Process natural language input and convert to MongoDB query"""
    data = request.get_json()
    user_input = data.get('input', '')
    
    if not user_input:
        return jsonify({'error': 'No input provided'})
    
    # Convert natural language to MongoDB query and execute
    mongodb_result = llm_processor.natural_language_to_mongodb(user_input)
    print(f"mongodb_result",mongodb_result)
    if not mongodb_result['success']:
        return jsonify({'error': mongodb_result['error']})
    
    response = {
        'approach': 'MongoDB',
        'original_input': user_input,
        'mongodb_query': mongodb_result['mongodb_query'],
        'query_result': mongodb_result['query_result']
    }
    
    return jsonify(response)

@app.route('/natural-language', methods=['POST'])
def natural_language_endpoint():
    """Process natural language with both GraphQL and MongoDB (default to GraphQL for compatibility)"""
    data = request.get_json()
    user_input = data.get('input', '')
    approach = data.get('approach', 'graphql').lower()  # 'graphql', 'mongodb', or 'both'
    
    if not user_input:
        return jsonify({'error': 'No input provided'})
    
    if approach == 'mongodb':
        return natural_language_mongodb_endpoint()
    elif approach == 'both':
        return natural_language_compare_endpoint()
    else:
        return natural_language_graphql_endpoint()

@app.route('/natural-language-compare', methods=['POST'])
def natural_language_compare_endpoint():
    """Compare GraphQL vs MongoDB approaches for the same natural language input"""
    data = request.get_json()
    user_input = data.get('input', '')
    
    if not user_input:
        return jsonify({'error': 'No input provided'})
    
    # Get comparison results
    comparison_result = llm_processor.compare_graphql_vs_mongodb(user_input)
    
    # Execute GraphQL query if successful using PyMongo schema
    if comparison_result['graphql_approach']['success']:
        try:
            graphql_result = schema.execute(comparison_result['graphql_approach']['graphql_query'])
            comparison_result['graphql_approach']['execution_result'] = {
                'data': graphql_result.data,
                'errors': [str(error) for error in graphql_result.errors] if graphql_result.errors else None
            }
        except Exception as e:
            comparison_result['graphql_approach']['execution_error'] = str(e)
    
    return jsonify(comparison_result)

@app.route('/mongodb-query', methods=['POST'])
def direct_mongodb_query():
    """Execute direct MongoDB query"""
    data = request.get_json()
    
    required_fields = ['collection', 'operation']
    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Missing required fields: collection, operation'})
    
    try:
        # Execute the MongoDB query using the processor
        result = llm_processor._execute_mongodb_query(data)
        
        return jsonify({
            'query': data,
            'result': result
        })
    
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'endpoints': {
            'graphql': '/graphql',
            'natural_language_graphql': '/natural-language-graphql',
            'natural_language_mongodb': '/natural-language-mongodb',
            'natural_language_compare': '/natural-language-compare',
            'direct_mongodb': '/mongodb-query'
        },
        'llm_processor': type(llm_processor).__name__,
        'mongodb_connected': llm_processor.db is not None,
        'graphql_backend': 'PyMongo (MongoEngine bypass)'  # Indicate we're using PyMongo
    })

# Keep all your other endpoints exactly the same...
@app.route('/sample-queries', methods=['GET'])
def sample_queries():
    """Provide sample queries for testing both approaches"""
    samples = {
        'natural_language_examples': [
            'Show me all action movies',
            'Find movies from 2010',
            'Get movies with rating above 8.0',
            'Show all genres',
        ],
        'graphql_examples': [
            {
                'name': 'All Movies',
                'query': '''
                query {
                  allMoviesList {
                    title
                    year
                    rating
                    genres
                  }
                }
                '''
            },
            {
                'name': 'Movies by Genre',
                'query': '''
                query {
                  moviesByGenre(genre: "Action") {
                    title
                    year
                    rating
                    genres
                  }
                }
                '''
            }
        ],
        'mongodb_examples': [
            {
                'name': 'Find All Movies',
                'query': {
                    'collection': 'movies',
                    'operation': 'find',
                    'filter': {},
                    'projection': {'title': 1, 'year': 1, 'rating': 1}
                }
            },
            {
                'name': 'Find Action Movies',
                'query': {
                    'collection': 'movies',
                    'operation': 'find',
                    'filter': {'genres': {'$regex': 'Action', '$options': 'i'}},
                    'projection': {'title': 1, 'year': 1, 'rating': 1, 'genres': 1}
                }
            }
        ]
    }
    return jsonify(samples)

@app.route('/demo', methods=['GET'])
def demo_page():
    """Simple demo page showing both approaches"""
    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>GraphQL vs MongoDB Demo</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .container { max-width: 1200px; margin: 0 auto; }
            .demo-section { margin: 20px 0; padding: 20px; border: 1px solid #ddd; }
            textarea { width: 100%; height: 100px; }
            button { padding: 10px 20px; margin: 10px 0; }
            .result { background: #f5f5f5; padding: 10px; margin: 10px 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🎬 IMDB Database - GraphQL vs MongoDB (PyMongo Backend)</h1>
            
            <div class="demo-section">
                <h2>Natural Language Query</h2>
                <textarea id="userInput" placeholder="Enter your query (e.g., 'Show me all action movies')">Show me all action movies</textarea>
                <br>
                <button onclick="testGraphQL()">Test with GraphQL</button>
                <button onclick="testMongoDB()">Test with MongoDB</button>
                <button onclick="testBoth()">Compare Both</button>
                
                <div id="results" class="result"></div>
            </div>
        </div>
        
        <script>
            async function testGraphQL() {
                const input = document.getElementById('userInput').value;
                const response = await fetch('/natural-language-graphql', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({input: input})
                });
                const result = await response.json();
                document.getElementById('results').innerHTML = '<h3>GraphQL Result (PyMongo Backend):</h3><pre>' + JSON.stringify(result, null, 2) + '</pre>';
            }
            
            async function testMongoDB() {
                const input = document.getElementById('userInput').value;
                const response = await fetch('/natural-language-mongodb', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({input: input})
                });
                const result = await response.json();
                document.getElementById('results').innerHTML = '<h3>MongoDB Result:</h3><pre>' + JSON.stringify(result, null, 2) + '</pre>';
            }
            
            async function testBoth() {
                const input = document.getElementById('userInput').value;
                const response = await fetch('/natural-language-compare', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({input: input})
                });
                const result = await response.json();
                document.getElementById('results').innerHTML = '<h3>Comparison Result:</h3><pre>' + JSON.stringify(result, null, 2) + '</pre>';
            }
        </script>
    </body>
    </html>
    '''
    return html

if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5000))
    print(f"🚀 Starting Flask server on port {port}")
    print(f"📊 GraphQL endpoint: http://localhost:{port}/graphql (PyMongo backend)")
    print(f"🤖 Natural Language GraphQL: http://localhost:{port}/natural-language-graphql")
    print(f"🍃 Natural Language MongoDB: http://localhost:{port}/natural-language-mongodb")
    print(f"⚖️  Compare both approaches: http://localhost:{port}/natural-language-compare")
    print(f"🔧 Direct MongoDB queries: http://localhost:{port}/mongodb-query")
    print(f"🏥 Health check: http://localhost:{port}/health")
    print(f"📋 Sample queries: http://localhost:{port}/sample-queries")
    print(f"🎭 Demo page: http://localhost:{port}/demo")
    
    app.run(debug=True, host='0.0.0.0', port=port)