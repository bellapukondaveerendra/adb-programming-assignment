# app.py - FIXED ObjectId JSON serialization issue

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from graphene import Schema
from schema_pymongo import pymongo_schema as schema  # Use PyMongo schema
from llm_processor import LLMProcessor
import json
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

print("✅ Using PyMongo direct connection (bypassing MongoEngine)")

# Initialize LLM processor
try:
    llm_processor = LLMProcessor(model_name=os.getenv('OLLAMA_MODEL', 'llama2'))
    print("✅ LLM Processor initialized")
except Exception as e:
    print(f"⚠️  LLM Processor initialization warning: {e}")
    llm_processor = LLMProcessor()  # Will use fallback mode

def serialize_mongodb_result(obj):
    """
    Recursively convert ObjectId and other non-serializable objects to strings
    """
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: serialize_mongodb_result(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_mongodb_result(item) for item in obj]
    else:
        return obj

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

@app.route('/natural-language-mongodb', methods=['POST'])
def natural_language_mongodb_endpoint():
    """Process natural language input and convert to MongoDB query - FIXED ObjectId serialization"""
    data = request.get_json()
    user_input = data.get('input', '')
    
    if not user_input:
        return jsonify({'error': 'No input provided'})
    
    # Convert natural language to MongoDB query and execute
    mongodb_result = llm_processor.natural_language_to_mongodb(user_input)
    print(f"mongodb_result", mongodb_result)
    
    if not mongodb_result['success']:
        return jsonify({'error': mongodb_result['error']})
    
    # FIXED: Serialize the entire result to handle any ObjectIds
    serialized_result = serialize_mongodb_result(mongodb_result)
    
    response = {
        'approach': 'MongoDB',
        'original_input': user_input,
        'mongodb_query': serialized_result['mongodb_query'],
        'query_result': serialized_result['query_result']
    }
    
    # Additional metadata if available
    if 'parsed_query' in serialized_result:
        response['parsed_query'] = serialized_result['parsed_query']
    if 'parsing_method' in serialized_result:
        response['parsing_method'] = serialized_result['parsing_method']
    
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
    
    # FIXED: Serialize the comparison result to handle any ObjectIds
    serialized_comparison = serialize_mongodb_result(comparison_result)
    
    return jsonify(serialized_comparison)

@app.route('/mongodb-query', methods=['POST'])
def direct_mongodb_query():
    """Execute direct MongoDB query - FIXED ObjectId serialization"""
    data = request.get_json()
    
    required_fields = ['collection', 'operation']
    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Missing required fields: collection, operation'})
    
    try:
        # Execute the MongoDB query using the processor
        result = llm_processor._execute_mongodb_query(data)
        
        # FIXED: Serialize the result to handle any ObjectIds
        serialized_result = serialize_mongodb_result(result)
        
        response = {
            'query': data,
            'result': serialized_result
        }
        
        return jsonify(response)
    
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
        'graphql_backend': 'PyMongo (MongoEngine bypass)'
    })

@app.route('/sample-queries', methods=['GET'])
def sample_queries():
    """Provide sample queries for testing both approaches"""
    samples = {
        'natural_language_examples': [
            'Show me all action movies',
            'Find movies from 2010',
            'Get movies with rating above 8.0',
            'Show all genres',
            'Create a new movie with title "Veerendra"',
            'Count how many movies we have',
            'What is the average movie rating?'
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
                'name': 'Find all movies',
                'query': {
                    'collection': 'movies',
                    'operation': 'find',
                    'filter': {}
                }
            },
            {
                'name': 'Insert a movie',
                'query': {
                    'collection': 'movies',
                    'operation': 'insert_one',
                    'document': {
                        'title': 'Test Movie',
                        'year': 2024,
                        'rating': 8.5,
                        'genres': ['Action', 'Drama']
                    }
                }
            }
        ]
    }
    return jsonify(samples)

@app.route('/demo', methods=['GET'])
def demo_page():
    """Demo page to test natural language processing"""
    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Natural Language to Database Query Demo</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .container { max-width: 1000px; }
            input[type="text"] { width: 500px; padding: 10px; }
            button { padding: 10px 15px; margin: 5px; }
            #results { background: #f5f5f5; padding: 20px; margin-top: 20px; white-space: pre-wrap; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🤖 Natural Language to Database Query Demo</h1>
            <p>Try queries like: "Show me all action movies", "Find movies from 2010", "Create a new movie with title Inception"</p>
            
            <input type="text" id="userInput" placeholder="Enter your natural language query..." 
                   value="Create a new movie with title Veerendra">
            <br><br>
            
            <button onclick="testGraphQL()">🔍 Test GraphQL</button>
            <button onclick="testMongoDB()">🍃 Test MongoDB</button>
            <button onclick="testBoth()">⚖️ Compare Both</button>
            
            <div id="results"></div>
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
                document.getElementById('results').innerHTML = '<h3>MongoDB Result (FIXED):</h3><pre>' + JSON.stringify(result, null, 2) + '</pre>';
            }
            
            async function testBoth() {
                const input = document.getElementById('userInput').value;
                const response = await fetch('/natural-language-compare', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({input: input})
                });
                const result = await response.json();
                document.getElementById('results').innerHTML = '<h3>Comparison Result (FIXED):</h3><pre>' + JSON.stringify(result, null, 2) + '</pre>';
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