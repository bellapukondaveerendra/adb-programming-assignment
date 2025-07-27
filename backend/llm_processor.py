# # llm_processor.py - Final corrected version with proper LLM integration and array handling
# import re
# import json
# from typing import Dict, Any, List, Optional, Tuple
# from pymongo import MongoClient
# from mongoengine import connect
# from models import Movie, Genre
# import os
# import requests
# import time

# class LLMProcessor:
#     def __init__(self, model_name="llama2"):
#         self.model_name = model_name
#         self.ollama_base_url = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')
#         self.ollama_available = False
        
#         # Test Ollama connection
#         self._test_ollama_connection()
        
#         if self.ollama_available:
#             print(f"✅ Using Ollama LLM ({model_name}) with rule-based fallback")
#         else:
#             print(f"⚠️  Ollama not available - using rule-based processor only")
        
#         # MongoDB connection for direct queries
#         self.mongodb_uri = os.getenv('MONGODB_URI')
#         self.mongo_client = None
#         self.db = None
#         self.mongodb_connected = False
        
#         if self.mongodb_uri:
#             try:
#                 self.mongo_client = MongoClient(self.mongodb_uri)
#                 # Test the connection
#                 self.mongo_client.admin.command('ping')
#                 self.db = self.mongo_client.imdb
#                 self.mongodb_connected = True
#                 print("✅ Connected to MongoDB for direct queries")
#             except Exception as e:
#                 print(f"⚠️  MongoDB connection failed: {e}")
#                 self.mongodb_connected = False
#         else:
#             print("⚠️  MongoDB URI not found - MongoDB queries will be simulated")
        
#         # Define rule-based patterns as fallback
#         self.operation_patterns = {
#             'CREATE': r'(add|create|insert|new)\s+(movie|film|genre)',
#             'READ': r'(show|get|find|list|search|display|what|which|tell me)',
#             'UPDATE': r'(update|modify|change|edit|set)',
#             'DELETE': r'(delete|remove|drop)',
#             'COUNT': r'(count|how many|number of)',
#             'AGGREGATE': r'(average|mean|sum|total|max|min|highest|lowest)'
#         }
        
#         # Entity patterns
#         self.entity_patterns = {
#             'MOVIE': r'(movie|movies|film|films)',
#             'GENRE': r'(genre|genres|category|categories)',
#             'DIRECTOR': r'(director|directors)',
#             'ACTOR': r'(actor|actors|star|stars)'
#         }
    
#     def _test_ollama_connection(self):
#         """Test if Ollama is running and accessible"""
#         try:
#             response = requests.get(f"{self.ollama_base_url}/api/tags", timeout=5)
#             if response.status_code == 200:
#                 # Check if our model is available
#                 models = response.json().get('models', [])
#                 available_models = [model.get('name', '') for model in models]
                
#                 # Look for exact match or compatible model
#                 for available_model in available_models:
#                     if self.model_name in available_model:
#                         self.ollama_available = True
#                         print(f"✅ Found model: {available_model}")
#                         return
                
#                 # If model not found, try to use any available model
#                 if available_models:
#                     # Prefer faster models
#                     preferred_models = ['llama2', 'phi3', 'qwen2.5', 'tinyllama']
#                     for preferred in preferred_models:
#                         for available in available_models:
#                             if preferred in available.lower():
#                                 self.model_name = available.split(':')[0]
#                                 self.ollama_available = True
#                                 print(f"🔄 Using alternative model: {self.model_name}")
#                                 return
                    
#                     # Use first available model as last resort
#                     self.model_name = available_models[0].split(':')[0]
#                     self.ollama_available = True
#                     print(f"🔄 Using available model: {self.model_name}")
#                 else:
#                     print(f"⚠️  No models found. Run: ollama pull {self.model_name}")
#             else:
#                 print(f"⚠️  Ollama server responded with status: {response.status_code}")
#         except Exception as e:
#             print(f"⚠️  Cannot connect to Ollama: {e}")
#             print("Make sure Ollama is running: ollama serve")
    
#     def _call_ollama(self, prompt: str, system_prompt: str = None) -> Dict[str, Any]:
#         """Call Ollama API with error handling"""
#         try:
#             payload = {
#                 "model": self.model_name,
#                 "prompt": prompt,
#                 "stream": False,
#                 "options": {
#                     "temperature": 0.1,
#                     "top_p": 0.9,
#                     "num_predict": 150
#                 }
#             }
            
#             if system_prompt:
#                 payload["system"] = system_prompt
            
#             start_time = time.time()
            
#             response = requests.post(
#                 f"{self.ollama_base_url}/api/generate",
#                 json=payload,
#                 timeout=25  # Shorter timeout for faster fallback
#             )
            
#             elapsed = time.time() - start_time
            
#             if response.status_code == 200:
#                 result = response.json()
#                 return {
#                     'success': True,
#                     'response': result.get('response', ''),
#                     'model': result.get('model', self.model_name),
#                     'elapsed_time': elapsed
#                 }
#             else:
#                 return {
#                     'success': False,
#                     'error': f"Ollama API error: {response.status_code}"
#                 }
                
#         except requests.exceptions.Timeout:
#             return {
#                 'success': False,
#                 'error': f"Ollama request timed out. Try a smaller model like 'llama2'"
#             }
#         except Exception as e:
#             return {
#                 'success': False,
#                 'error': f"Ollama call failed: {str(e)}"
#             }
    
#     def parse_natural_language_with_llm(self, user_input: str) -> Dict[str, Any]:
#         """Parse natural language using Ollama LLM"""
        
#         system_prompt = """Parse movie database queries into JSON. Return only JSON.

# Schema: Movies(title, genres[], year, rating, directors[]), Genres(name)
# Operations: READ, CREATE, UPDATE, DELETE, COUNT, AGGREGATE

# Return format:
# {"operation": "READ", "entity": "MOVIE", "filters": {"genre": "Action"}}

# For UPDATE operations, use this format:
# {"operation": "UPDATE", "entity": "MOVIE", "filters": {"title": "OldTitle"}, "updates": {"title": "NewTitle", "rating": 9.0}}

# For rating filters, use this format:
# {"operation": "READ", "entity": "MOVIE", "filters": {"rating": {"operator": "above", "value": 6}}}

# Examples:
# "action movies" → {"operation": "READ", "entity": "MOVIE", "filters": {"genre": "Action"}}
# "movies with rating above 6" → {"operation": "READ", "entity": "MOVIE", "filters": {"rating": {"operator": "above", "value": 6}}}
# "update Inception title to Inception 2" → {"operation": "UPDATE", "entity": "MOVIE", "filters": {"title": "Inception"}, "updates": {"title": "Inception 2"}}
# "count movies" → {"operation": "COUNT", "entity": "MOVIE", "filters": {}}"""

#         prompt = f"Query: '{user_input}'\nJSON:"
        
#         llm_result = self._call_ollama(prompt, system_prompt)
        
#         if llm_result['success']:
#             try:
#                 # Extract JSON from response - FIXED to handle nested objects
#                 response_text = llm_result['response'].strip()
#                 print(f"🤖 LLM response ({llm_result.get('elapsed_time', 0):.1f}s): {response_text[:100]}...")
                
#                 # Try multiple extraction methods
#                 parsed_json = None
                
#                 # Method 1: Look for complete JSON with proper bracket matching
#                 bracket_count = 0
#                 start_idx = -1
#                 for i, char in enumerate(response_text):
#                     if char == '{':
#                         if start_idx == -1:
#                             start_idx = i
#                         bracket_count += 1
#                     elif char == '}':
#                         bracket_count -= 1
#                         if bracket_count == 0 and start_idx != -1:
#                             json_str = response_text[start_idx:i+1]
#                             try:
#                                 parsed_json = json.loads(json_str)
#                                 print(f"✅ Extracted JSON with bracket matching: {json_str}")
#                                 break
#                             except json.JSONDecodeError:
#                                 continue
                
#                 # Method 2: If bracket matching failed, try regex patterns
#                 if not parsed_json:
#                     json_patterns = [
#                         r'\{[^{}]*\{[^{}]*\}[^{}]*\}',  # Nested objects
#                         r'\{[^{}]*\}',                   # Simple objects
#                     ]
                    
#                     for pattern in json_patterns:
#                         json_match = re.search(pattern, response_text, re.DOTALL)
#                         if json_match:
#                             try:
#                                 json_str = json_match.group(0)
#                                 parsed_json = json.loads(json_str)
#                                 print(f"✅ Extracted JSON with regex: {json_str}")
#                                 break
#                             except json.JSONDecodeError:
#                                 continue
                
#                 # Method 3: Try to find JSON anywhere in the response
#                 if not parsed_json:
#                     lines = response_text.split('\n')
#                     for line in lines:
#                         line = line.strip()
#                         if line.startswith('{') and line.endswith('}'):
#                             try:
#                                 parsed_json = json.loads(line)
#                                 print(f"✅ Extracted JSON from line: {line}")
#                                 break
#                             except json.JSONDecodeError:
#                                 continue
                
#                 if parsed_json and self._validate_and_normalize_json(parsed_json):
#                     return {
#                         'success': True,
#                         'parsed_query': parsed_json,
#                         'method': 'ollama_llm',
#                         'original_input': user_input,
#                         'elapsed_time': llm_result.get('elapsed_time', 0)
#                     }
                
#                 print(f"⚠️  Could not extract valid JSON from: {response_text}")
#                 print(f"🔍 Debug - Full response: '{response_text}'")
#                 return self._fallback_to_rules(user_input)
                    
#             except json.JSONDecodeError as e:
#                 print(f"⚠️  JSON parsing error: {e}")
#                 return self._fallback_to_rules(user_input)
#         else:
#             print(f"⚠️  LLM call failed: {llm_result['error']}")
#             return self._fallback_to_rules(user_input)
    
#     def _validate_and_normalize_json(self, parsed: Dict[str, Any]) -> bool:
#         """Validate and normalize parsed JSON"""
        
#         # Check required fields
#         if 'operation' not in parsed or 'entity' not in parsed:
#             print(f"⚠️  Missing required fields in: {parsed}")
#             return False
        
#         # Normalize operation
#         operation = parsed['operation'].upper()
#         valid_operations = ['READ', 'CREATE', 'UPDATE', 'DELETE', 'COUNT', 'AGGREGATE']
#         if operation not in valid_operations:
#             print(f"⚠️  Invalid operation: {operation}")
#             return False
        
#         # Normalize entity
#         entity = parsed['entity'].upper()
#         valid_entities = ['MOVIE', 'GENRE']
#         if entity not in valid_entities:
#             print(f"⚠️  Invalid entity: {entity}")
#             return False
        
#         # Update with normalized values
#         parsed['operation'] = operation
#         parsed['entity'] = entity
        
#         # Ensure filters exists
#         if 'filters' not in parsed:
#             parsed['filters'] = {}
        
#         print(f"✅ Valid JSON: operation={operation}, entity={entity}")
#         return True
    
#     def _fallback_to_rules(self, user_input: str) -> Dict[str, Any]:
#         """Fallback to rule-based parsing when LLM fails"""
#         print("🔄 Falling back to rule-based parsing")
        
#         user_input_lower = user_input.lower().strip()
        
#         # Determine operation using rules
#         operation = self._determine_operation_rules(user_input_lower)
#         entity = self._determine_entity_rules(user_input_lower)
#         filters = self._extract_filters_rules_enhanced(user_input)
        
#         parsed_query = {
#             'operation': operation,
#             'entity': entity,
#             'filters': filters
#         }
        
#         # Add operation details for aggregations
#         if operation == 'AGGREGATE':
#             if 'average' in user_input_lower or 'mean' in user_input_lower:
#                 parsed_query['operation_details'] = {
#                     'aggregate_function': 'avg',
#                     'aggregate_field': 'rating'
#                 }
        
#         return {
#             'success': True,
#             'parsed_query': parsed_query,
#             'method': 'rule_based_fallback',
#             'original_input': user_input
#         }
    
#     def natural_language_to_graphql(self, user_input: str) -> Dict[str, Any]:
#         """Convert natural language to GraphQL using LLM or rules"""
        
#         try:
#             # Try LLM first if available
#             if self.ollama_available:
#                 parsed_result = self.parse_natural_language_with_llm(user_input)
#             else:
#                 parsed_result = self._fallback_to_rules(user_input)
            
#             if not parsed_result['success']:
#                 return {
#                     'success': False,
#                     'error': 'Failed to parse natural language input',
#                     'original_input': user_input
#                 }
            
#             parsed = parsed_result['parsed_query']
            
#             # Generate GraphQL based on parsed components
#             graphql_query = self._generate_graphql_query(parsed)
            
#             return {
#                 'success': True,
#                 'graphql_query': graphql_query,
#                 'original_input': user_input,
#                 'parsed_query': parsed,
#                 'parsing_method': parsed_result['method']
#             }
            
#         except Exception as e:
#             return {
#                 'success': False,
#                 'error': str(e),
#                 'original_input': user_input
#             }
    
#     def natural_language_to_mongodb(self, user_input: str) -> Dict[str, Any]:
#         """Convert natural language to MongoDB query using LLM or rules"""
        
#         try:
#             # Try LLM first if available
#             if self.ollama_available:
#                 parsed_result = self.parse_natural_language_with_llm(user_input)
#             else:
#                 parsed_result = self._fallback_to_rules(user_input)
            
#             if not parsed_result['success']:
#                 return {
#                     'success': False,
#                     'error': 'Failed to parse natural language input',
#                     'original_input': user_input
#                 }
            
#             parsed = parsed_result['parsed_query']
            
#             # Generate MongoDB query based on parsed components
#             mongo_query = self._generate_mongodb_query(parsed)
            
#             print(f"Generated MongoDB query: {mongo_query}")
            
#             # Execute the MongoDB query
#             result = self._execute_mongodb_query(mongo_query)
            
#             return {
#                 'success': True,
#                 'mongodb_query': mongo_query,
#                 'query_result': result,
#                 'original_input': user_input,
#                 'parsed_query': parsed,
#                 'parsing_method': parsed_result['method']
#             }
            
#         except Exception as e:
#             print(f"Error in natural_language_to_mongodb: {e}")
#             return {
#                 'success': False,
#                 'error': str(e),
#                 'original_input': user_input
#             }
    
#     def _generate_graphql_query(self, parsed: Dict[str, Any]) -> str:
#         """Generate GraphQL query from parsed components"""
        
#         operation = parsed.get('operation', 'READ')
#         entity = parsed.get('entity', 'MOVIE')
#         filters = parsed.get('filters', {})
        
#         if operation == 'CREATE':
#             return self._generate_graphql_mutation(parsed)
#         elif operation == 'UPDATE':
#             return self._generate_graphql_mutation(parsed)
#         elif operation == 'DELETE':
#             return self._generate_graphql_mutation(parsed)
#         else:  # READ, COUNT, AGGREGATE
#             return self._generate_graphql_query_read(parsed)
    
#     # def _generate_graphql_query_read(self, parsed: Dict[str, Any]) -> str:
#     #     """Generate GraphQL read query"""
        
#     #     entity = parsed.get('entity', 'MOVIE')
#     #     filters = parsed.get('filters', {})
        
#     #     if entity == 'MOVIE':
#     #         if not filters:
#     #             return '''
#     #             query {
#     #               allMovies {
#     #                 edges {
#     #                   node {
#     #                     title
#     #                     year
#     #                     rating
#     #                     genre
#     #                     directors
#     #                   }
#     #                 }
#     #               }
#     #             }
#     #             '''
#     #         elif 'genre' in filters:
#     #             genre = filters['genre']
#     #             return f'''
#     #             query {{
#     #               moviesByGenre(genre: "{genre}") {{
#     #                 title
#     #                 year
#     #                 rating
#     #                 genre
#     #                 directors
#     #               }}
#     #             }}
#     #             '''
#     #         elif 'year' in filters:
#     #             year = filters['year']
#     #             return f'''
#     #             query {{
#     #               moviesByYear(year: {year}) {{
#     #                 title
#     #                 year
#     #                 rating
#     #                 genre
#     #                 directors
#     #               }}
#     #             }}
#     #             '''
#     #         elif 'rating' in filters and isinstance(filters['rating'], dict):
#     #             rating = filters['rating']['value']
#     #             return f'''
#     #             query {{
#     #               moviesByRating(minRating: {rating}) {{
#     #                 title
#     #                 year
#     #                 rating
#     #                 genre
#     #                 directors
#     #               }}
#     #             }}
#     #             '''
#     #         else:
#     #             return '''
#     #             query {
#     #               allMovies {
#     #                 edges {
#     #                   node {
#     #                     title
#     #                     year
#     #                     rating
#     #                     genre
#     #                     directors
#     #                   }
#     #                 }
#     #               }
#     #             }
#     #             '''
                
#     #     elif entity == 'GENRE':
#     #         return '''
#     #         query {
#     #           allGenres {
#     #             edges {
#     #               node {
#     #                 name
#     #                 description
#     #                 movieCount
#     #               }
#     #             }
#     #           }
#     #         }
#     #         '''
        
#     #     # Default fallback
#     #     return '''
#     #     query {
#     #       allMovies {
#     #         edges {
#     #           node {
#     #             title
#     #             year
#     #             rating
#     #           }
#     #         }
#     #       }
#     #     }
#     #     '''
    
    

#     # Update this method in your llm_processor.py
#     def _generate_graphql_query_read(self, parsed: Dict[str, Any]) -> str:
#         """Generate GraphQL read query with correct field names for PyMongo schema - FIXED for complex filters"""
        
#         entity = parsed.get('entity', 'MOVIE')
#         filters = parsed.get('filters', {})
        
#         if entity == 'MOVIE':
#             if not filters:
#                 return '''
#                 query {
#                 allMoviesList {
#                     title
#                     year
#                     rating
#                     genres
#                     directors
#                 }
#                 }
#                 '''
#             elif 'genre' in filters:
#                 genre = filters['genre']
#                 return f'''
#                 query {{
#                 moviesByGenre(genre: "{genre}") {{
#                     title
#                     year
#                     rating
#                     genres
#                     directors
#                     runtime
#                 }}
#                 }}
#                 '''
#             elif 'year' in filters:
#                 # FIXED: Handle complex year filter objects
#                 year_filter = filters['year']
#                 if isinstance(year_filter, dict):
#                     # Extract the actual year value from complex filter
#                     if 'value' in year_filter:
#                         year = year_filter['value']
#                     elif 'year' in year_filter:
#                         year = year_filter['year']
#                     else:
#                         # Fallback - try to find any numeric value
#                         year = next((v for v in year_filter.values() if isinstance(v, (int, float))), 2020)
#                 else:
#                     year = year_filter
                
#                 return f'''
#                 query {{
#                 moviesByYear(year: {year}) {{
#                     title
#                     year
#                     rating
#                     genres
#                     directors
#                 }}
#                 }}
#                 '''
#             elif 'rating' in filters:
#                 # FIXED: Handle complex rating filter objects  
#                 rating_filter = filters['rating']
#                 if isinstance(rating_filter, dict):
#                     # Extract the actual rating value
#                     if 'value' in rating_filter:
#                         rating = rating_filter['value']
#                     elif 'rating' in rating_filter:
#                         rating = rating_filter['rating']
#                     else:
#                         # Fallback - try to find any numeric value
#                         rating = next((v for v in rating_filter.values() if isinstance(v, (int, float))), 8.0)
#                 else:
#                     rating = rating_filter
                
#                 return f'''
#                 query {{
#                 moviesByRating(minRating: {rating}) {{
#                     title
#                     year
#                     rating
#                     genres
#                     directors
#                 }}
#                 }}
#                 '''
#             else:
#                 return '''
#                 query {
#                 allMoviesList {
#                     title
#                     year
#                     rating
#                     genres
#                     directors
#                 }
#                 }
#                 '''
                
#         elif entity == 'GENRE':
#             return '''
#             query {
#             allMoviesList {
#                 genres
#             }
#             }
#             '''
        
#         # Default fallback
#         return '''
#         query {
#         allMoviesList {
#             title
#             year
#             rating
#             genres
#         }
#         }
#         '''

#     def _generate_graphql_mutation(self, parsed: Dict[str, Any]) -> str:
#         """Generate GraphQL mutation"""
        
#         operation = parsed.get('operation')
#         entity = parsed.get('entity')
#         filters = parsed.get('filters', {})
        
#         if operation == 'CREATE' and entity == 'MOVIE':
#             title = filters.get('title', 'New Movie')
#             return f'''
#             mutation {{
#               createMovie(title: "{title}") {{
#                 movie {{
#                   title
#                   year
#                   rating
#                 }}
#               }}
#             }}
#             '''
#         elif operation == 'CREATE' and entity == 'GENRE':
#             name = filters.get('title', 'New Genre')
#             return f'''
#             mutation {{
#               createGenre(name: "{name}") {{
#                 genre {{
#                   name
#                   description
#                 }}
#               }}
#             }}
#             '''
        
#         return '''
#         query {
#           allMovies {
#             edges {
#               node {
#                 title
#                 year
#                 rating
#               }
#             }
#           }
#         }
#         '''
    
#     def _generate_mongodb_query(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
#         """Generate MongoDB query from parsed components - FIXED update handling"""
        
#         operation = parsed.get('operation', 'READ')
#         entity = parsed.get('entity', 'MOVIE')
#         filters = parsed.get('filters', {})
#         updates = parsed.get('updates', {})  # NEW: Handle updates field
#         operation_details = parsed.get('operation_details', {})
        
#         # Determine collection
#         collection = 'movies' if entity == 'MOVIE' else 'genres'
        
#         # Build MongoDB filter
#         mongo_filter = self._build_mongodb_filter(filters)
        
#         # Generate query based on operation
#         if operation == 'READ':
#             return {
#                 'collection': collection,
#                 'operation': 'find',
#                 'filter': mongo_filter,
#                 'projection': self._get_projection(entity),
#                 'limit': 20 if not filters else None
#             }
#         elif operation == 'COUNT':
#             return {
#                 'collection': collection,
#                 'operation': 'count_documents',
#                 'filter': mongo_filter
#             }
#         elif operation == 'AGGREGATE':
#             return self._build_aggregate_query(operation_details, mongo_filter, collection)
#         elif operation == 'CREATE':
#             return self._build_create_query(entity, filters)
#         elif operation == 'UPDATE':
#             return self._build_update_query_fixed(entity, filters, updates, mongo_filter)
#         elif operation == 'DELETE':
#             return {
#                 'collection': collection,
#                 'operation': 'delete_one',
#                 'filter': mongo_filter
#             }
        
#         # Default fallback
#         return {
#             'collection': 'movies',
#             'operation': 'find',
#             'filter': {},
#             'projection': {'title': 1, 'year': 1, 'rating': 1},
#             'limit': 10
#         }
    
#     def _build_update_query_fixed(self, entity: str, filters: Dict, updates: Dict, mongo_filter: Dict) -> Dict[str, Any]:
#         """Build update query with proper updates handling"""
        
#         update_doc = {'$set': {}}
        
#         # Use the updates field if provided by LLM
#         if updates:
#             for key, value in updates.items():
#                 if key == 'genre':
#                     # Map to correct field name
#                     update_doc['$set']['genres'] = [value] if isinstance(value, str) else value
#                 else:
#                     update_doc['$set'][key] = value
#         else:
#             # Fallback to old method if no updates field
#             for key, value in filters.items():
#                 if key not in ['title', 'id']:  # Don't update filter fields
#                     if key == 'rating' and isinstance(value, dict) and 'value' in value:
#                         update_doc['$set']['rating'] = value['value']
#                     elif key == 'genre':
#                         update_doc['$set']['genres'] = [value] if isinstance(value, str) else value
#                     else:
#                         update_doc['$set'][key] = value
        
#         # If still no updates, add a timestamp
#         if not update_doc['$set']:
#             update_doc['$set']['last_updated'] = 'auto_updated'
        
#         collection = 'movies' if entity == 'MOVIE' else 'genres'
        
#         return {
#             'collection': collection,
#             'operation': 'update_one',
#             'filter': mongo_filter or {'title': {'$regex': filters.get('title', ''), '$options': 'i'}},
#             'update': update_doc
#         }
    
#     # def _build_mongodb_filter(self, filters: Dict[str, Any]) -> Dict[str, Any]:
#     #     """Build MongoDB filter from extracted filters - FIXED field names and rating handling"""
#     #     mongo_filter = {}
        
#     #     for filter_name, filter_value in filters.items():
#     #         if filter_name == 'title':
#     #             # Title field
#     #             mongo_filter['title'] = {'$regex': filter_value, '$options': 'i'}
#     #         elif filter_name == 'genre':
#     #             # Map 'genre' filter to 'genres' field (plural) in movies collection
#     #             mongo_filter['genres'] = {'$regex': filter_value, '$options': 'i'}
#     #         elif filter_name == 'year':
#     #             mongo_filter['year'] = filter_value
#     #         elif filter_name == 'rating':
#     #             # Handle different rating filter formats
#     #             if isinstance(filter_value, dict):
#     #                 # Handle our expected format: {"operator": "above", "value": 6}
#     #                 if 'operator' in filter_value and 'value' in filter_value:
#     #                     operator = filter_value['operator']
#     #                     value = filter_value['value']
#     #                     if operator in ['above', 'over', '>', '>=']:
#     #                         mongo_filter['rating'] = {'$gte': value}
#     #                     elif operator in ['below', 'under', '<', '<=']:
#     #                         mongo_filter['rating'] = {'$lte': value}
#     #                     elif operator in ['equals', '=', '==']:
#     #                         mongo_filter['rating'] = value
#     #                 # Handle LLM's direct MongoDB format: {"$gt": 6}
#     #                 elif '$gt' in filter_value:
#     #                     mongo_filter['rating'] = {'$gte': filter_value['$gt']}
#     #                 elif '$gte' in filter_value:
#     #                     mongo_filter['rating'] = {'$gte': filter_value['$gte']}
#     #                 elif '$lt' in filter_value:
#     #                     mongo_filter['rating'] = {'$lte': filter_value['$lt']}
#     #                 elif '$lte' in filter_value:
#     #                     mongo_filter['rating'] = {'$lte': filter_value['$lte']}
#     #                 elif '$eq' in filter_value:
#     #                     mongo_filter['rating'] = filter_value['$eq']
#     #                 else:
#     #                     # If it's some other dict format, just use it as-is
#     #                     mongo_filter['rating'] = filter_value
#     #             else:
#     #                 # Direct value
#     #                 mongo_filter['rating'] = filter_value
#     #         elif filter_name == 'director':
#     #             # Map to 'directors' field (plural)
#     #             mongo_filter['directors'] = {'$regex': re.escape(filter_value), '$options': 'i'}
#     #         elif filter_name == 'actor':
#     #             # Map to 'actors' field (plural)
#     #             mongo_filter['actors'] = {'$regex': re.escape(filter_value), '$options': 'i'}
        
#     #     return mongo_filter
    
#     def _build_mongodb_filter(self, filters: Dict[str, Any]) -> Dict[str, Any]:
#         """Build MongoDB filter from extracted filters - FIXED for plural field names"""
#         mongo_filter = {}
        
#         for filter_name, filter_value in filters.items():
#             if filter_name == 'title':
#                 # Title field
#                 mongo_filter['title'] = {'$regex': filter_value, '$options': 'i'}
#             elif filter_name == 'genre':
#                 # FIXED: Map 'genre' filter to 'genres' field (plural) in movies collection
#                 mongo_filter['genres'] = {'$regex': filter_value, '$options': 'i'}
#             elif filter_name == 'year':
#                 mongo_filter['year'] = filter_value
#             elif filter_name == 'rating':
#                 # Handle different rating filter formats
#                 if isinstance(filter_value, dict):
#                     # Handle our expected format: {"operator": "above", "value": 6}
#                     if 'operator' in filter_value and 'value' in filter_value:
#                         operator = filter_value['operator']
#                         value = filter_value['value']
#                         if operator in ['above', 'over', '>', '>=']:
#                             mongo_filter['rating'] = {'$gte': value}
#                         elif operator in ['below', 'under', '<', '<=']:
#                             mongo_filter['rating'] = {'$lte': value}
#                         elif operator in ['equals', '=', '==']:
#                             mongo_filter['rating'] = value
#                     # Handle LLM's direct MongoDB format: {"$gt": 6}
#                     elif '$gt' in filter_value:
#                         mongo_filter['rating'] = {'$gte': filter_value['$gt']}
#                     elif '$gte' in filter_value:
#                         mongo_filter['rating'] = {'$gte': filter_value['$gte']}
#                     elif '$lt' in filter_value:
#                         mongo_filter['rating'] = {'$lte': filter_value['$lt']}
#                     elif '$lte' in filter_value:
#                         mongo_filter['rating'] = {'$lte': filter_value['$lte']}
#                     elif '$eq' in filter_value:
#                         mongo_filter['rating'] = filter_value['$eq']
#                     else:
#                         # If it's some other dict format, just use it as-is
#                         mongo_filter['rating'] = filter_value
#                 else:
#                     # Direct value
#                     mongo_filter['rating'] = filter_value
#             elif filter_name == 'director':
#                 # Map to 'directors' field (plural)
#                 mongo_filter['directors'] = {'$regex': re.escape(filter_value), '$options': 'i'}
#             elif filter_name == 'actor':
#                 # Map to 'actors' field (plural)
#                 mongo_filter['actors'] = {'$regex': re.escape(filter_value), '$options': 'i'}
        
#         return mongo_filter
    


#     def _build_mongodb_filter_enhanced(self, filters: Dict[str, Any]) -> Dict[str, Any]:
#         """Build MongoDB filter with enhanced rating support including ranges"""
#         mongo_filter = {}
        
#         for filter_name, filter_value in filters.items():
#             if filter_name == 'title':
#                 mongo_filter['title'] = {'$regex': filter_value, '$options': 'i'}
#             elif filter_name == 'genre':
#                 mongo_filter['genres'] = {'$regex': filter_value, '$options': 'i'}
#             elif filter_name == 'year':
#                 mongo_filter['year'] = filter_value
#             elif filter_name == 'rating':
#                 # Enhanced rating filter handling
#                 if isinstance(filter_value, dict):
#                     if 'operator' in filter_value:
#                         operator = filter_value['operator']
#                         if operator == 'above':
#                             mongo_filter['rating'] = {'$gte': filter_value['value']}
#                         elif operator == 'below':
#                             mongo_filter['rating'] = {'$lte': filter_value['value']}
#                         elif operator == 'range':
#                             # NEW: Handle range queries
#                             mongo_filter['rating'] = {
#                                 '$gte': filter_value['min'],
#                                 '$lte': filter_value['max']
#                             }
#                         elif operator == 'equals':
#                             mongo_filter['rating'] = filter_value['value']
#                     # Handle direct MongoDB operators
#                     elif '$gt' in filter_value:
#                         mongo_filter['rating'] = {'$gte': filter_value['$gt']}
#                     elif '$gte' in filter_value:
#                         mongo_filter['rating'] = {'$gte': filter_value['$gte']}
#                     elif '$lt' in filter_value:
#                         mongo_filter['rating'] = {'$lte': filter_value['$lt']}
#                     elif '$lte' in filter_value:
#                         mongo_filter['rating'] = {'$lte': filter_value['$lte']}
#                     elif '$eq' in filter_value:
#                         mongo_filter['rating'] = filter_value['$eq']
#                     else:
#                         mongo_filter['rating'] = filter_value
#                 else:
#                     # Direct value - exact match
#                     mongo_filter['rating'] = filter_value
#             elif filter_name == 'director':
#                 mongo_filter['directors'] = {'$regex': re.escape(filter_value), '$options': 'i'}
#             elif filter_name == 'actor':
#                 mongo_filter['actors'] = {'$regex': re.escape(filter_value), '$options': 'i'}
        
#         return mongo_filter
#     def _build_aggregate_query(self, details: Dict, mongo_filter: Dict, collection: str) -> Dict[str, Any]:
#         """Build MongoDB aggregation query"""
        
#         agg_function = details.get('aggregate_function', 'avg')
#         agg_field = details.get('aggregate_field', 'rating')
        
#         pipeline = []
        
#         # Add match stage if there are filters
#         if mongo_filter:
#             pipeline.append({'$match': mongo_filter})
        
#         # Add group stage
#         group_stage = {'_id': None}
        
#         if agg_function == 'avg':
#             group_stage[f'average_{agg_field}'] = {'$avg': f'${agg_field}'}
#         elif agg_function == 'sum':
#             group_stage[f'total_{agg_field}'] = {'$sum': f'${agg_field}'}
#         elif agg_function == 'max':
#             group_stage[f'max_{agg_field}'] = {'$max': f'${agg_field}'}
#         elif agg_function == 'min':
#             group_stage[f'min_{agg_field}'] = {'$min': f'${agg_field}'}
        
#         pipeline.append({'$group': group_stage})
#         pipeline.append({'$project': {'_id': 0}})
        
#         return {
#             'collection': collection,
#             'operation': 'aggregate',
#             'pipeline': pipeline
#         }
    
#     # def _build_create_query(self, entity: str, filters: Dict) -> Dict[str, Any]:
#     #     """Build create query"""
        
#     #     if entity == 'MOVIE':
#     #         document = {
#     #             'title': filters.get('title', 'New Movie'),
#     #             'year': filters.get('year', 2024),
#     #             'rating': 0.0,
#     #             'genre': [filters.get('genre', 'Unknown')] if 'genre' in filters else ['Unknown'],
#     #             'directors': [filters.get('director', 'Unknown')] if 'director' in filters else ['Unknown'],
#     #             'actors': [filters.get('actor', 'Unknown')] if 'actor' in filters else ['Unknown']
#     #         }
#     #         return {
#     #             'collection': 'movies',
#     #             'operation': 'insert_one',
#     #             'document': document
#     #         }
#     #     else:
#     #         return {
#     #             'collection': 'genres',
#     #             'operation': 'insert_one',
#     #             'document': {
#     #                 'name': filters.get('title', 'New Genre'),
#     #                 'description': f"Genre: {filters.get('title', 'New Genre')}",
#     #                 'movie_count': 0
#     #             }
#     #         }
    

#     def _build_create_query(self, entity: str, filters: Dict) -> Dict[str, Any]:
#         """Build create query with correct field names"""
        
#         if entity == 'MOVIE':
#             document = {
#                 'title': filters.get('title', 'New Movie'),
#                 'year': filters.get('year', 2024),
#                 'rating': 0.0,
#                 'genres': [filters.get('genre', 'Unknown')] if 'genre' in filters else ['Unknown'],  # Use 'genres' (plural)
#                 'directors': [filters.get('director', 'Unknown')] if 'director' in filters else ['Unknown'],
#                 'actors': [filters.get('actor', 'Unknown')] if 'actor' in filters else ['Unknown'],
#                 'runtime': filters.get('runtime', 0),  # Use 'runtime' not 'runtime_minutes'
#                 'revenue': filters.get('revenue', 0.0)  # Use 'revenue' not 'revenue_millions'
#             }
#             return {
#                 'collection': 'movies',
#                 'operation': 'insert_one',
#                 'document': document
#             }
#         else:
#             return {
#                 'collection': 'genres',
#                 'operation': 'insert_one',
#                 'document': {
#                     'name': filters.get('title', 'New Genre'),
#                     'description': f"Genre: {filters.get('title', 'New Genre')}",
#                     'movie_count': 0
#                 }
#             }
    

#     def _build_update_query(self, entity: str, filters: Dict, mongo_filter: Dict) -> Dict[str, Any]:
#         """Build update query - FIXED to handle new values properly"""
        
#         update_doc = {'$set': {}}
        
#         # Extract what to update vs what to filter by
#         # Look for new values in different possible fields
#         possible_update_fields = ['new_title', 'new_value', 'new_rating', 'new_year', 'new_genre']
        
#         for key, value in filters.items():
#             # Skip fields used for filtering (finding the document)
#             if key in ['title', 'id']:
#                 continue
                
#             # Handle new value fields
#             if key in possible_update_fields:
#                 # Map new field names to actual database fields
#                 if key == 'new_title':
#                     update_doc['$set']['title'] = value
#                 elif key == 'new_value':
#                     # Generic new value - try to determine what field to update
#                     # This is a fallback when LLM doesn't specify the exact field
#                     if 'title' in str(filters.get('title', '')).lower():
#                         update_doc['$set']['title'] = value
#                     else:
#                         update_doc['$set']['title'] = value  # Default to title
#                 elif key == 'new_rating':
#                     update_doc['$set']['rating'] = float(value) if isinstance(value, (str, int)) else value
#                 elif key == 'new_year':
#                     update_doc['$set']['year'] = int(value) if isinstance(value, (str, float)) else value
#                 elif key == 'new_genre':
#                     update_doc['$set']['genres'] = [value] if isinstance(value, str) else value
#             else:
#                 # For other fields, only update if they're not used for filtering
#                 if key == 'rating' and isinstance(value, (int, float)):
#                     update_doc['$set']['rating'] = value
#                 elif key == 'year' and isinstance(value, (int, str)):
#                     update_doc['$set']['year'] = int(value) if isinstance(value, str) else value
#                 elif key == 'genre' and key not in mongo_filter:
#                     update_doc['$set']['genres'] = [value] if isinstance(value, str) else value
        
#         # If no updates were found, try to extract from the original parsing
#         if not update_doc['$set']:
#             # This is a fallback - maybe the LLM put the new value in an unexpected place
#             print(f"⚠️  No update fields found in filters: {filters}")
#             # Add a default update to prevent empty $set
#             update_doc['$set']['updated_at'] = 'auto_updated'
        
#         collection = 'movies' if entity == 'MOVIE' else 'genres'
        
#         return {
#             'collection': collection,
#             'operation': 'update_one',
#             'filter': mongo_filter or {'title': {'$regex': filters.get('title', ''), '$options': 'i'}},
#             'update': update_doc
#         }
    
#     # def _get_projection(self, entity: str) -> Dict[str, int]:
#     #     """Get appropriate projection for entity - FIXED field names"""
        
#     #     if entity == 'MOVIE':
#     #         # Use correct plural field names as they exist in the database
#     #         return {'title': 1, 'year': 1, 'rating': 1, 'genres': 1, 'directors': 1}
#     #     elif entity == 'GENRE':
#     #         return {'name': 1, 'description': 1, 'movie_count': 1}
#     #     else:
#     #         return {}
    
#     def _get_projection(self, entity: str) -> Dict[str, int]:
#         """Get appropriate projection for entity with correct field names"""
        
#         if entity == 'MOVIE':
#             return {
#                 'title': 1, 
#                 'year': 1, 
#                 'rating': 1, 
#                 'genres': 1,  # Use 'genres' (plural)
#                 'directors': 1,
#                 'runtime': 1,  # Use 'runtime' not 'runtime_minutes'
#                 'revenue': 1   # Use 'revenue' not 'revenue_millions'
#             }
#         elif entity == 'GENRE':
#             return {'name': 1, 'description': 1, 'movie_count': 1}
#         else:
#             return {}

#     # Rule-based fallback methods
#     def _determine_operation_rules(self, text: str) -> str:
#         """Determine operation using rule-based approach"""
#         for operation, pattern in self.operation_patterns.items():
#             if re.search(pattern, text, re.IGNORECASE):
#                 return operation
#         return 'READ'
    
#     def _determine_entity_rules(self, text: str) -> str:
#         """Determine entity using rule-based approach"""
#         for entity, pattern in self.entity_patterns.items():
#             if re.search(pattern, text, re.IGNORECASE):
#                 return entity
#         return 'MOVIE'
    
#     def _extract_filters_rules(self, text: str) -> Dict[str, Any]:
#         """Extract filters using rule-based approach"""
#         filters = {}
        
#         # Extract genre
#         genre_match = re.search(r'\b(action|comedy|drama|thriller|horror|sci-fi|romance|fantasy|adventure|crime)\b', text, re.IGNORECASE)
#         if genre_match:
#             filters['genre'] = genre_match.group().title()
        
#         # Extract year
#         year_match = re.search(r'\b(19|20)\d{2}\b', text)
#         if year_match:
#             filters['year'] = int(year_match.group())
        
#         # Extract rating
#         rating_match = re.search(r'rating\s*(above|over|>|>=)\s*(\d+\.?\d*)', text, re.IGNORECASE)
#         if rating_match:
#             filters['rating'] = {
#                 'operator': rating_match.group(1),
#                 'value': float(rating_match.group(2))
#             }
        
#         # Extract title
#         title_match = re.search(r'(?:called|named|titled)\s+["\']([^"\']+)["\']', text, re.IGNORECASE)
#         if not title_match:
#             title_match = re.search(r'(?:called|named|titled)\s+([A-Za-z0-9\s]+?)(?:\s|$)', text, re.IGNORECASE)
#         if title_match:
#             filters['title'] = title_match.group(1).strip()
        
#         # Extract director
#         director_match = re.search(r'(?:directed by|director)\s+([A-Za-z\s]+?)(?:\s|$)', text, re.IGNORECASE)
#         if director_match:
#             filters['director'] = director_match.group(1).strip()
        
#         return filters
    

#     def _extract_filters_rules_enhanced(self, text: str) -> Dict[str, Any]:
#         """Enhanced filter extraction with comprehensive rating pattern matching"""
#         filters = {}
        
#         # Extract genre
#         genre_match = re.search(r'\b(action|comedy|drama|thriller|horror|sci-fi|romance|fantasy|adventure|crime|documentary|animation|biography|history|mystery|war|western|musical|sport)\b', text, re.IGNORECASE)
#         if genre_match:
#             filters['genre'] = genre_match.group().title()
        
#         # Extract year
#         year_match = re.search(r'\b(19|20)\d{2}\b', text)
#         if year_match:
#             filters['year'] = int(year_match.group())
        
#         # ENHANCED: Comprehensive rating extraction
#         rating_patterns = [
#             # Comparative operators - more variations
#             (r'rating\s*(?:is\s+)?(?:above|over|greater\s+than|>|>=)\s*(\d+\.?\d*)', 'above'),
#             (r'rating\s*(?:is\s+)?(?:below|under|less\s+than|<|<=)\s*(\d+\.?\d*)', 'below'),
            
#             # Exact matches - multiple patterns
#             (r'with\s+rating\s+(\d+\.?\d*)', 'exact'),
#             (r'rating\s+(?:is\s+|equals?\s+|=\s*)?(\d+\.?\d*)', 'exact'),
#             (r'rated\s+(\d+\.?\d*)', 'exact'),
#             (r'(?:^|\s)(\d+\.?\d*)\s+rating', 'exact'),  # "8.1 rating"
#             (r'rating:\s*(\d+\.?\d*)', 'exact'),  # "rating: 8.1"
            
#             # Range patterns (future enhancement)
#             (r'rating\s+between\s+(\d+\.?\d*)\s+and\s+(\d+\.?\d*)', 'range'),
#         ]
        
#         for pattern, operation_type in rating_patterns:
#             match = re.search(pattern, text, re.IGNORECASE)
#             if match:
#                 if operation_type == 'above':
#                     filters['rating'] = {
#                         'operator': 'above',
#                         'value': float(match.group(1))
#                     }
#                 elif operation_type == 'below':
#                     filters['rating'] = {
#                         'operator': 'below', 
#                         'value': float(match.group(1))
#                     }
#                 elif operation_type == 'exact':
#                     filters['rating'] = float(match.group(1))
#                 elif operation_type == 'range':
#                     filters['rating'] = {
#                         'operator': 'range',
#                         'min': float(match.group(1)),
#                         'max': float(match.group(2))
#                     }
#                 break  # Use first match found
        
#         # Extract title
#         title_patterns = [
#             r'(?:called|named|titled)\s+["\']([^"\']+)["\']',
#             r'(?:called|named|titled)\s+([A-Za-z0-9\s]+?)(?:\s|$)',
#             r'movie\s+["\']([^"\']+)["\']',
#             r'film\s+["\']([^"\']+)["\']'
#         ]
        
#         for pattern in title_patterns:
#             title_match = re.search(pattern, text, re.IGNORECASE)
#             if title_match:
#                 filters['title'] = title_match.group(1).strip()
#                 break
        
#         # Extract director
#         director_match = re.search(r'(?:directed\s+by|director\s*:?)\s+([A-Za-z\s]+?)(?:\s|$)', text, re.IGNORECASE)
#         if director_match:
#             filters['director'] = director_match.group(1).strip()
        
#         # Extract actor
#         actor_match = re.search(r'(?:starring|actor\s*:?|stars?)\s+([A-Za-z\s]+?)(?:\s|$)', text, re.IGNORECASE)
#         if actor_match:
#             filters['actor'] = actor_match.group(1).strip()
        
#         return filters


#     def _execute_mongodb_query(self, query_info: Dict[str, Any]) -> Any:
#         """Execute MongoDB query and return results"""
        
#         if not self.mongodb_connected or self.db is None:
#             return {
#                 'error': 'MongoDB connection not available', 
#                 'simulated': True,
#                 'note': 'Please check your MongoDB connection string in .env file'
#             }
        
#         try:
#             collection = self.db[query_info['collection']]
#             operation = query_info['operation']
            
#             print(f"Executing {operation} on collection {query_info['collection']}")
            
#             if operation == 'find':
#                 cursor = collection.find(
#                     query_info['filter'],
#                     query_info.get('projection')
#                 )
                
#                 if 'limit' in query_info and query_info['limit']:
#                     cursor = cursor.limit(query_info['limit'])
                
#                 results = list(cursor)
                
#                 for result in results:
#                     if '_id' in result:
#                         result['_id'] = str(result['_id'])
                
#                 return {
#                     'results': results,
#                     'count': len(results),
#                     'operation': operation
#                 }
                
#             elif operation == 'aggregate':
#                 results = list(collection.aggregate(query_info['pipeline']))
#                 return {
#                     'results': results,
#                     'operation': operation
#                 }
                
#             elif operation == 'insert_one':
#                 result = collection.insert_one(query_info['document'])
#                 return {
#                     'inserted_id': str(result.inserted_id),
#                     'acknowledged': result.acknowledged,
#                     'operation': operation
#                 }
                
#             elif operation == 'update_one':
#                 result = collection.update_one(
#                     query_info['filter'],
#                     query_info['update']
#                 )
#                 return {
#                     'matched_count': result.matched_count,
#                     'modified_count': result.modified_count,
#                     'operation': operation
#                 }
                
#             elif operation == 'delete_one':
#                 result = collection.delete_one(query_info['filter'])
#                 return {
#                     'deleted_count': result.deleted_count,
#                     'operation': operation
#                 }
                
#             else:
#                 return {'error': f'Unsupported operation: {operation}'}
                
#         except Exception as e:
#             print(f"MongoDB execution error: {e}")
#             return {'error': f'MongoDB execution failed: {str(e)}'}
    
#     def compare_graphql_vs_mongodb(self, user_input: str) -> Dict[str, Any]:
#         """Compare GraphQL and MongoDB approaches for the same query"""
        
#         graphql_result = self.natural_language_to_graphql(user_input)
#         mongodb_result = self.natural_language_to_mongodb(user_input)
        
#         return {
#             'user_input': user_input,
#             'graphql_approach': graphql_result,
#             'mongodb_approach': mongodb_result,
#             'comparison': {
#                 'graphql_success': graphql_result.get('success', False),
#                 'mongodb_success': mongodb_result.get('success', False),
#                 'both_successful': graphql_result.get('success', False) and mongodb_result.get('success', False)
#             }
#         }
    
#     def get_connection_status(self) -> Dict[str, Any]:
#         """Get connection status information"""
#         return {
#             'mongodb_connected': self.mongodb_connected,
#             'mongodb_uri_provided': self.mongodb_uri is not None,
#             'database_name': 'imdb' if self.mongodb_connected else None,
#             'ollama_available': self.ollama_available,
#             'ollama_model': self.model_name
#         }



# llm_processor.py - COMPLETE FIXED VERSION
import re
import json
from typing import Dict, Any, List, Optional, Tuple
from pymongo import MongoClient
from mongoengine import connect
from models import Movie, Genre
import os
import requests
import time

class LLMProcessor:
    def __init__(self, model_name="llama2"):
        self.model_name = model_name
        self.ollama_base_url = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')
        self.ollama_available = False
        
        # Test Ollama connection
        self._test_ollama_connection()
        
        if self.ollama_available:
            print(f"✅ Using Ollama LLM ({model_name}) with rule-based fallback")
        else:
            print(f"⚠️  Ollama not available - using rule-based processor only")
        
        # MongoDB connection for direct queries
        self.mongodb_uri = os.getenv('MONGODB_URI')
        self.mongo_client = None
        self.db = None
        self.mongodb_connected = False
        
        if self.mongodb_uri:
            try:
                self.mongo_client = MongoClient(self.mongodb_uri)
                # Test the connection
                self.mongo_client.admin.command('ping')
                self.db = self.mongo_client.imdb
                self.mongodb_connected = True
                print("✅ Connected to MongoDB for direct queries")
            except Exception as e:
                print(f"⚠️  MongoDB connection failed: {e}")
                self.mongodb_connected = False
        else:
            print("⚠️  MongoDB URI not found - MongoDB queries will be simulated")
        
        # FIXED: Improved operation patterns
        self.operation_patterns = {
            'DELETE': r'\b(delete|remove|drop|eliminate|destroy)\s+(movie|film|genre)',
            'CREATE': r'\b(add|create|insert|new|make)\s+(movie|film|genre)',
            'UPDATE': r'\b(update|modify|change|edit|set|alter)\s+(movie|film|genre)',
            'READ': r'\b(show|get|find|list|search|display|what|which|tell me|view)',
            'COUNT': r'\b(count|how many|number of)',
            'AGGREGATE': r'\b(average|mean|sum|total|max|min|highest|lowest)'
        }
        
        # Entity patterns
        self.entity_patterns = {
            'MOVIE': r'\b(movie|movies|film|films)',
            'GENRE': r'\b(genre|genres|category|categories)',
            'DIRECTOR': r'\b(director|directors)',
            'ACTOR': r'\b(actor|actors|star|stars)'
        }
    
    def _test_ollama_connection(self):
        """Test if Ollama is running and accessible"""
        try:
            response = requests.get(f"{self.ollama_base_url}/api/tags", timeout=5)
            if response.status_code == 200:
                # Check if our model is available
                models = response.json().get('models', [])
                available_models = [model.get('name', '') for model in models]
                
                # Look for exact match or compatible model
                for available_model in available_models:
                    if self.model_name in available_model:
                        self.ollama_available = True
                        print(f"✅ Found model: {available_model}")
                        return
                
                # If model not found, try to use any available model
                if available_models:
                    # Prefer faster models
                    preferred_models = ['llama2', 'phi3', 'qwen2.5', 'tinyllama']
                    for preferred in preferred_models:
                        for available in available_models:
                            if preferred in available.lower():
                                self.model_name = available.split(':')[0]
                                self.ollama_available = True
                                print(f"🔄 Using alternative model: {self.model_name}")
                                return
                    
                    # Use first available model as last resort
                    self.model_name = available_models[0].split(':')[0]
                    self.ollama_available = True
                    print(f"🔄 Using available model: {self.model_name}")
                else:
                    print(f"⚠️  No models found. Run: ollama pull {self.model_name}")
            else:
                print(f"⚠️  Ollama server responded with status: {response.status_code}")
        except Exception as e:
            print(f"⚠️  Cannot connect to Ollama: {e}")
            print("Make sure Ollama is running: ollama serve")
    
    def _call_ollama(self, prompt: str, system_prompt: str = None) -> Dict[str, Any]:
        """Call Ollama API with error handling"""
        try:
            payload = {
                "model": self.model_name,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "top_p": 0.9,
                    "num_predict": 150
                }
            }
            
            if system_prompt:
                payload["system"] = system_prompt
            
            start_time = time.time()
            
            response = requests.post(
                f"{self.ollama_base_url}/api/generate",
                json=payload,
                timeout=25  # Shorter timeout for faster fallback
            )
            
            elapsed = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                return {
                    'success': True,
                    'response': result.get('response', ''),
                    'model': result.get('model', self.model_name),
                    'elapsed_time': elapsed
                }
            else:
                return {
                    'success': False,
                    'error': f"Ollama API error: {response.status_code}"
                }
                
        except requests.exceptions.Timeout:
            return {
                'success': False,
                'error': f"Ollama request timed out. Try a smaller model like 'llama2'"
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Ollama call failed: {str(e)}"
            }
    
    def parse_natural_language_with_llm(self, user_input: str) -> Dict[str, Any]:
        """Parse natural language using Ollama LLM with improved DELETE detection"""
        
        system_prompt = """Parse movie database queries into JSON. Return only JSON.

Schema: Movies(title, genres[], year, rating, directors[]), Genres(name)
Operations: READ, CREATE, UPDATE, DELETE, COUNT, AGGREGATE

IMPORTANT: Pay attention to operation keywords:
- DELETE words: delete, remove, drop, eliminate, destroy
- CREATE words: add, create, insert, new, make  
- UPDATE words: update, modify, change, edit, set
- READ words: show, get, find, list, search, display

Return format:
{"operation": "DELETE", "entity": "MOVIE", "filters": {"title": "Deadpool"}}

Examples:
"delete movie Deadpool" → {"operation": "DELETE", "entity": "MOVIE", "filters": {"title": "Deadpool"}}
"remove film called Avatar" → {"operation": "DELETE", "entity": "MOVIE", "filters": {"title": "Avatar"}}
"add movie Inception" → {"operation": "CREATE", "entity": "MOVIE", "data": {"title": "Inception"}}
"update movie Inception rating to 9.0" → {"operation": "UPDATE", "entity": "MOVIE", "filters": {"title": "Inception"}, "updates": {"rating": 9.0}}
"show action movies" → {"operation": "READ", "entity": "MOVIE", "filters": {"genre": "Action"}}
"movies with rating 8.1" → {"operation": "READ", "entity": "MOVIE", "filters": {"rating": 8.1}}"""

        prompt = f"Query: '{user_input}'\nJSON:"
        
        llm_result = self._call_ollama(prompt, system_prompt)
        
        if llm_result['success']:
            try:
                # Extract JSON from response - FIXED to handle nested objects
                response_text = llm_result['response'].strip()
                print(f"🤖 LLM response ({llm_result.get('elapsed_time', 0):.1f}s): {response_text[:100]}...")
                
                # Try multiple extraction methods
                parsed_json = None
                
                # Method 1: Look for complete JSON with proper bracket matching
                bracket_count = 0
                start_idx = -1
                for i, char in enumerate(response_text):
                    if char == '{':
                        if start_idx == -1:
                            start_idx = i
                        bracket_count += 1
                    elif char == '}':
                        bracket_count -= 1
                        if bracket_count == 0 and start_idx != -1:
                            json_str = response_text[start_idx:i+1]
                            try:
                                parsed_json = json.loads(json_str)
                                print(f"✅ Extracted JSON with bracket matching: {json_str}")
                                break
                            except json.JSONDecodeError:
                                continue
                
                # Method 2: If bracket matching failed, try regex patterns
                if not parsed_json:
                    json_patterns = [
                        r'\{[^{}]*\{[^{}]*\}[^{}]*\}',  # Nested objects
                        r'\{[^{}]*\}',                   # Simple objects
                    ]
                    
                    for pattern in json_patterns:
                        json_match = re.search(pattern, response_text, re.DOTALL)
                        if json_match:
                            try:
                                json_str = json_match.group(0)
                                parsed_json = json.loads(json_str)
                                print(f"✅ Extracted JSON with regex: {json_str}")
                                break
                            except json.JSONDecodeError:
                                continue
                
                if parsed_json and self._validate_and_normalize_json(parsed_json):
                    return {
                        'success': True,
                        'parsed_query': parsed_json,
                        'method': 'ollama_llm',
                        'original_input': user_input,
                        'elapsed_time': llm_result.get('elapsed_time', 0)
                    }
                
                print(f"⚠️  Could not extract valid JSON from: {response_text}")
                return self._fallback_to_rules(user_input)
                    
            except json.JSONDecodeError as e:
                print(f"⚠️  JSON parsing error: {e}")
                return self._fallback_to_rules(user_input)
        else:
            print(f"⚠️  LLM call failed: {llm_result['error']}")
            return self._fallback_to_rules(user_input)
    
    def _validate_and_normalize_json(self, parsed: Dict[str, Any]) -> bool:
        """Validate and normalize parsed JSON"""
        
        # Check required fields
        if 'operation' not in parsed or 'entity' not in parsed:
            print(f"⚠️  Missing required fields in: {parsed}")
            return False
        
        # Normalize operation
        operation = parsed['operation'].upper()
        valid_operations = ['READ', 'CREATE', 'UPDATE', 'DELETE', 'COUNT', 'AGGREGATE']
        if operation not in valid_operations:
            print(f"⚠️  Invalid operation: {operation}")
            return False
        
        # Normalize entity
        entity = parsed['entity'].upper()
        valid_entities = ['MOVIE', 'GENRE']
        if entity not in valid_entities:
            print(f"⚠️  Invalid entity: {entity}")
            return False
        
        # Update with normalized values
        parsed['operation'] = operation
        parsed['entity'] = entity
        
        # Ensure filters exists
        if 'filters' not in parsed:
            parsed['filters'] = {}
        
        print(f"✅ Valid JSON: operation={operation}, entity={entity}")
        return True
    
    def _fallback_to_rules(self, user_input: str) -> Dict[str, Any]:
        """Fallback to rule-based parsing when LLM fails"""
        print("🔄 Falling back to rule-based parsing")
        
        user_input_lower = user_input.lower().strip()
        
        # Determine operation using rules
        operation = self._determine_operation_rules(user_input_lower)
        entity = self._determine_entity_rules(user_input_lower)
        filters = self._extract_filters_rules(user_input)
        
        parsed_query = {
            'operation': operation,
            'entity': entity,
            'filters': filters
        }
        
        # Add operation details for aggregations
        if operation == 'AGGREGATE':
            if 'average' in user_input_lower or 'mean' in user_input_lower:
                parsed_query['operation_details'] = {
                    'aggregate_function': 'avg',
                    'aggregate_field': 'rating'
                }
        
        return {
            'success': True,
            'parsed_query': parsed_query,
            'method': 'rule_based_fallback',
            'original_input': user_input
        }
    
    def natural_language_to_graphql(self, user_input: str) -> Dict[str, Any]:
        """Convert natural language to GraphQL using LLM or rules"""
        
        try:
            # Try LLM first if available
            if self.ollama_available:
                parsed_result = self.parse_natural_language_with_llm(user_input)
            else:
                parsed_result = self._fallback_to_rules(user_input)
            
            if not parsed_result['success']:
                return {
                    'success': False,
                    'error': 'Failed to parse natural language input',
                    'original_input': user_input
                }
            
            parsed = parsed_result['parsed_query']
            
            # Generate GraphQL based on parsed components
            graphql_query = self._generate_graphql_query(parsed)
            
            return {
                'success': True,
                'graphql_query': graphql_query,
                'original_input': user_input,
                'parsed_query': parsed,
                'parsing_method': parsed_result['method']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'original_input': user_input
            }
    
    def natural_language_to_mongodb(self, user_input: str) -> Dict[str, Any]:
        """Convert natural language to MongoDB query using LLM or rules"""
        
        try:
            # Try LLM first if available
            if self.ollama_available:
                parsed_result = self.parse_natural_language_with_llm(user_input)
            else:
                parsed_result = self._fallback_to_rules(user_input)
            
            if not parsed_result['success']:
                return {
                    'success': False,
                    'error': 'Failed to parse natural language input',
                    'original_input': user_input
                }
            
            parsed = parsed_result['parsed_query']
            
            # Generate MongoDB query based on parsed components
            mongo_query = self._generate_mongodb_query(parsed)
            
            print(f"Generated MongoDB query: {mongo_query}")
            
            # Execute the MongoDB query
            result = self._execute_mongodb_query(mongo_query)
            
            return {
                'success': True,
                'mongodb_query': mongo_query,
                'query_result': result,
                'original_input': user_input,
                'parsed_query': parsed,
                'parsing_method': parsed_result['method']
            }
            
        except Exception as e:
            print(f"Error in natural_language_to_mongodb: {e}")
            return {
                'success': False,
                'error': str(e),
                'original_input': user_input
            }
    
    def _generate_graphql_query(self, parsed: Dict[str, Any]) -> str:
        """Generate GraphQL query from parsed components - FIXED to route operations correctly"""
        
        operation = parsed.get('operation', 'READ')
        
        # FIXED: Route DELETE/CREATE/UPDATE to mutations, not queries
        if operation in ['CREATE', 'UPDATE', 'DELETE']:
            return self._generate_graphql_mutation(parsed)
        else:  # READ, COUNT, AGGREGATE
            return self._generate_graphql_query_read(parsed)
    
    def _generate_graphql_query_read(self, parsed: Dict[str, Any]) -> str:
        """Generate GraphQL read query with correct field names for PyMongo schema"""
        
        entity = parsed.get('entity', 'MOVIE')
        filters = parsed.get('filters', {})
        
        if entity == 'MOVIE':
            if not filters:
                return '''
                query {
                  allMoviesList {
                    title
                    year
                    rating
                    genres
                    directors
                  }
                }
                '''
            elif 'title' in filters:
                title = filters['title']
                return f'''
                query {{
                  movieByTitle(title: "{title}") {{
                    title
                    year
                    rating
                    genres
                    directors
                  }}
                }}
                '''
            elif 'genre' in filters:
                genre = filters['genre']
                return f'''
                query {{
                  moviesByGenre(genre: "{genre}") {{
                    title
                    year
                    rating
                    genres
                    directors
                    runtime
                  }}
                }}
                '''
            elif 'year' in filters:
                # FIXED: Handle complex year filter objects
                year_filter = filters['year']
                if isinstance(year_filter, dict):
                    # Extract the actual year value from complex filter
                    if 'value' in year_filter:
                        year = year_filter['value']
                    elif 'year' in year_filter:
                        year = year_filter['year']
                    else:
                        # Fallback - try to find any numeric value
                        year = next((v for v in year_filter.values() if isinstance(v, (int, float))), 2020)
                else:
                    year = year_filter
                
                return f'''
                query {{
                  moviesByYear(year: {year}) {{
                    title
                    year
                    rating
                    genres
                    directors
                  }}
                }}
                '''
            elif 'rating' in filters:
                # FIXED: Handle complex rating filter objects  
                rating_filter = filters['rating']
                if isinstance(rating_filter, dict):
                    # Extract the actual rating value
                    if 'value' in rating_filter:
                        rating = rating_filter['value']
                    elif 'rating' in rating_filter:
                        rating = rating_filter['rating']
                    else:
                        # Fallback - try to find any numeric value
                        rating = next((v for v in rating_filter.values() if isinstance(v, (int, float))), 8.0)
                else:
                    rating = rating_filter
                
                return f'''
                query {{
                  moviesByRating(minRating: {rating}) {{
                    title
                    year
                    rating
                    genres
                    directors
                  }}
                }}
                '''
            else:
                return '''
                query {
                  allMoviesList {
                    title
                    year
                    rating
                    genres
                    directors
                  }
                }
                '''
                
        elif entity == 'GENRE':
            return '''
            query {
              allGenresList {
                name
                description
              }
            }
            '''
        
        # Default fallback
        return '''
        query {
          allMoviesList {
            title
            year
            rating
            genres
          }
        }
        '''

    def _generate_graphql_mutation(self, parsed: Dict[str, Any]) -> str:
        """Generate GraphQL mutation for CREATE, UPDATE, DELETE operations"""
        
        operation = parsed.get('operation')
        entity = parsed.get('entity')
        filters = parsed.get('filters', {})
        updates = parsed.get('updates', {})
        
        if operation == 'DELETE':
            if entity == 'MOVIE':
                title = filters.get('title', '')
                if title:
                    return f'''
                    mutation {{
                      deleteMovieByTitle(title: "{title}") {{
                        result {{
                          success
                          message
                        }}
                      }}
                    }}
                    '''
                else:
                    return '''
                    query {
                      error: "Cannot delete movie without specifying title"
                    }
                    '''
            
            elif entity == 'GENRE':
                name = filters.get('name') or filters.get('title', '')
                return f'''
                mutation {{
                  deleteGenreByName(name: "{name}") {{
                    result {{
                      success
                      message
                    }}
                  }}
                }}
                '''
        
        elif operation == 'CREATE':
            if entity == 'MOVIE':
                # Extract data for creation - FIXED to handle both LLM and rule-based parsing
                title = filters.get('title', 'New Movie')
                year = filters.get('year', 2024)
                rating = filters.get('rating', 0.0)
                genres = []
                directors = []
                
                # Handle genre
                if 'genre' in filters:
                    genres = [filters['genre']] if isinstance(filters['genre'], str) else filters['genre']
                else:
                    genres = ['Unknown']
                
                # Handle director
                if 'director' in filters:
                    directors = [filters['director']] if isinstance(filters['director'], str) else filters['director']
                else:
                    directors = ['Unknown']
                
                # Handle LLM data structure
                if 'data' in parsed:
                    data = parsed['data']
                    title = data.get('title', title)
                    year = data.get('year', year)
                    rating = data.get('rating', rating)
                    if 'genre' in data:
                        genres = [data['genre']] if isinstance(data['genre'], str) else data['genre']
                    if 'director' in data:
                        directors = [data['director']] if isinstance(data['director'], str) else data['director']
                
                # Format for GraphQL
                genres_str = json.dumps(genres)
                directors_str = json.dumps(directors)
                
                return f'''
                mutation {{
                  createMovie(
                    title: "{title}"
                    year: {year}
                    rating: {rating}
                    genres: {genres_str}
                    directors: {directors_str}
                  ) {{
                    movie {{
                      title
                      year
                      rating
                      genres
                      directors
                    }}
                  }}
                }}
                '''
            
            elif entity == 'GENRE':
                name = filters.get('title') or filters.get('name', 'New Genre')
                description = filters.get('description', f"Genre: {name}")
                
                if 'data' in parsed and 'name' in parsed['data']:
                    name = parsed['data']['name']
                elif 'data' in parsed and 'title' in parsed['data']:
                    name = parsed['data']['title']
                
                return f'''
                mutation {{
                  createGenre(name: "{name}", description: "{description}") {{
                    genre {{
                      name
                      description
                    }}
                  }}
                }}
                '''
        
        elif operation == 'UPDATE':
            if entity == 'MOVIE':
                title = filters.get('title', '')
                update_fields = []
                
                # Build update fields from updates dict
                for field, value in updates.items():
                    if field == 'title':
                        update_fields.append(f'title: "{value}"')
                    elif field == 'year':
                        update_fields.append(f'year: {value}')
                    elif field == 'rating':
                        update_fields.append(f'rating: {value}')
                    elif field == 'genre':
                        genres = [value] if isinstance(value, str) else value
                        update_fields.append(f'genres: {json.dumps(genres)}')
                
                if update_fields and title:
                    fields_str = ', '.join(update_fields)
                    return f'''
                    mutation {{
                      updateMovieByTitle(title: "{title}", {fields_str}) {{
                        movie {{
                          title
                          year
                          rating
                          genres
                          directors
                        }}
                      }}
                    }}
                    '''
        
        # Fallback
        return '''
        query {
          error: "Could not generate appropriate mutation"
        }
        '''
    
    def _generate_mongodb_query(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Generate MongoDB query from parsed components - FIXED to pass parsed data"""
        
        operation = parsed.get('operation', 'READ')
        entity = parsed.get('entity', 'MOVIE')
        filters = parsed.get('filters', {})
        updates = parsed.get('updates', {})
        operation_details = parsed.get('operation_details', {})
        
        # Determine collection
        collection = 'movies' if entity == 'MOVIE' else 'genres'
        
        # Build MongoDB filter
        mongo_filter = self._build_mongodb_filter(filters)
        
        # Generate query based on operation
        if operation == 'READ':
            return {
                'collection': collection,
                'operation': 'find',
                'filter': mongo_filter,
                'projection': self._get_projection(entity),
                'limit': 20 if not filters else None
            }
        elif operation == 'COUNT':
            return {
                'collection': collection,
                'operation': 'count_documents',
                'filter': mongo_filter
            }
        elif operation == 'AGGREGATE':
            return self._build_aggregate_query(operation_details, mongo_filter, collection)
        elif operation == 'CREATE':
            # FIXED: Pass the entire parsed object, not just filters
            return self._build_create_query(entity, filters, parsed)
        elif operation == 'UPDATE':
            return self._build_update_query_fixed(entity, filters, updates, mongo_filter)
        elif operation == 'DELETE':
            return {
                'collection': collection,
                'operation': 'delete_one',
                'filter': mongo_filter
            }
        
        # Default fallback
        return {
            'collection': 'movies',
            'operation': 'find',
            'filter': {},
            'projection': {'title': 1, 'year': 1, 'rating': 1},
            'limit': 10
        }
    
    def _build_create_query(self, entity: str, filters: Dict, parsed_data: Dict = None) -> Dict[str, Any]:
        """Build create query with ALL properties from LLM parsing - UNIVERSAL FIX"""
        
        if entity == 'MOVIE':
            # Start with intelligent defaults
            document = {
                'title': 'New Movie',
                'year': 2024,
                'rating': 0.0,
                'genres': ['Unknown'],
                'directors': ['Unknown'],
                'actors': ['Unknown'],
                'runtime': 0,
                'revenue': 0.0
            }
            
            # STEP 1: Apply LLM extracted data (highest priority)
            if parsed_data and 'data' in parsed_data:
                llm_data = parsed_data['data']
                print(f"🤖 LLM extracted data: {llm_data}")
                
                # Map ALL possible fields the LLM might extract
                field_mappings = {
                    # Direct field mappings
                    'title': 'title',
                    'year': 'year', 
                    'rating': 'rating',
                    'runtime': 'runtime',
                    'revenue': 'revenue',
                    
                    # Singular to plural mappings
                    'genre': 'genres',      # genre -> genres[]
                    'director': 'directors', # director -> directors[]
                    'actor': 'actors',      # actor -> actors[]
                    
                    # Alternative field names LLM might use
                    'name': 'title',        # LLM sometimes uses 'name' instead of 'title'
                    'imdb_rating': 'rating',
                    'release_year': 'year',
                    'film_title': 'title'
                }
                
                for llm_field, llm_value in llm_data.items():
                    if llm_field in field_mappings:
                        db_field = field_mappings[llm_field]
                        
                        # Handle array fields (genres, directors, actors)
                        if db_field in ['genres', 'directors', 'actors']:
                            if isinstance(llm_value, list):
                                document[db_field] = llm_value
                            else:
                                document[db_field] = [llm_value]  # Convert single value to array
                        else:
                            # Handle scalar fields (title, year, rating, etc.)
                            document[db_field] = llm_value
                    else:
                        # Unknown field - add it anyway (flexible schema)
                        print(f"⚠️  Unknown field from LLM: {llm_field} = {llm_value}")
                        document[llm_field] = llm_value
            
            # STEP 2: Apply rule-based filters (lower priority, won't overwrite LLM data)
            rule_mappings = {
                'title': 'title',
                'year': 'year',
                'rating': 'rating', 
                'runtime': 'runtime',
                'revenue': 'revenue',
                'genre': 'genres',
                'director': 'directors',
                'actor': 'actors'
            }
            
            for rule_field, rule_value in filters.items():
                if rule_field in rule_mappings:
                    db_field = rule_mappings[rule_field]
                    
                    # Only apply if LLM didn't already set this field (check for defaults)
                    if (db_field == 'title' and document[db_field] == 'New Movie') or \
                       (db_field in ['genres', 'directors', 'actors'] and document[db_field] == ['Unknown']) or \
                       (db_field in ['year', 'rating', 'runtime', 'revenue'] and document[db_field] in [2024, 0.0, 0]):
                        
                        if db_field in ['genres', 'directors', 'actors']:
                            document[db_field] = [rule_value] if not isinstance(rule_value, list) else rule_value
                        else:
                            document[db_field] = rule_value
            
            print(f"🎬 Final document to create: {document}")
            
            return {
                'collection': 'movies',
                'operation': 'insert_one',
                'document': document
            }
        
        else:  # GENRE entity
            # Similar logic for genres
            document = {
                'name': 'New Genre',
                'description': '',
                'movie_count': 0
            }
            
            # Apply LLM data
            if parsed_data and 'data' in parsed_data:
                llm_data = parsed_data['data']
                
                genre_mappings = {
                    'name': 'name',
                    'title': 'name',  # LLM might use 'title' for genre name
                    'description': 'description'
                }
                
                for llm_field, llm_value in llm_data.items():
                    if llm_field in genre_mappings:
                        db_field = genre_mappings[llm_field]
                        document[db_field] = llm_value
            
            # Apply rule-based filters
            if 'title' in filters:
                document['name'] = filters['title']
            if 'name' in filters:
                document['name'] = filters['name']
            if 'description' in filters:
                document['description'] = filters['description']
            
            # Auto-generate description if not provided
            if not document['description']:
                document['description'] = f"Genre: {document['name']}"
            
            return {
                'collection': 'genres',
                'operation': 'insert_one',
                'document': document
            }
    
    def _build_update_query_fixed(self, entity: str, filters: Dict, updates: Dict, mongo_filter: Dict) -> Dict[str, Any]:
        """Build update query with proper updates handling"""
        
        update_doc = {'$set': {}}
        
        # Use the updates field if provided by LLM
        if updates:
            for key, value in updates.items():
                if key == 'genre':
                    # Map to correct field name
                    update_doc['$set']['genres'] = [value] if isinstance(value, str) else value
                else:
                    update_doc['$set'][key] = value
        else:
            # Fallback to old method if no updates field
            for key, value in filters.items():
                if key not in ['title', 'id']:  # Don't update filter fields
                    if key == 'rating' and isinstance(value, dict) and 'value' in value:
                        update_doc['$set']['rating'] = value['value']
                    elif key == 'genre':
                        update_doc['$set']['genres'] = [value] if isinstance(value, str) else value
                    else:
                        update_doc['$set'][key] = value
        
        # If still no updates, add a timestamp
        if not update_doc['$set']:
            update_doc['$set']['last_updated'] = 'auto_updated'
        
        collection = 'movies' if entity == 'MOVIE' else 'genres'
        
        return {
            'collection': collection,
            'operation': 'update_one',
            'filter': mongo_filter or {'title': {'$regex': filters.get('title', ''), '$options': 'i'}},
            'update': update_doc
        }
    
    def _build_mongodb_filter(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build MongoDB filter from extracted filters - FIXED rating and field names"""
        mongo_filter = {}
        
        for filter_name, filter_value in filters.items():
            if filter_name == 'title':
                # Title field
                mongo_filter['title'] = {'$regex': filter_value, '$options': 'i'}
            elif filter_name == 'genre':
                # FIXED: Map 'genre' filter to 'genres' field (plural) in movies collection
                mongo_filter['genres'] = {'$regex': filter_value, '$options': 'i'}
            elif filter_name == 'year':
                mongo_filter['year'] = filter_value
            elif filter_name == 'rating':
                # IMPROVED: Handle different rating filter formats
                if isinstance(filter_value, dict):
                    # Handle our expected format: {"operator": "above", "value": 6}
                    if 'operator' in filter_value and 'value' in filter_value:
                        operator = filter_value['operator']
                        value = filter_value['value']
                        if operator in ['above', 'over', '>', '>=']:
                            mongo_filter['rating'] = {'$gte': value}
                        elif operator in ['below', 'under', '<', '<=']:
                            mongo_filter['rating'] = {'$lte': value}
                        elif operator in ['equals', '=', '==']:
                            mongo_filter['rating'] = value
                    # Handle LLM's direct MongoDB format: {"$gt": 6}
                    elif '$gt' in filter_value:
                        mongo_filter['rating'] = {'$gte': filter_value['$gt']}
                    elif '$gte' in filter_value:
                        mongo_filter['rating'] = {'$gte': filter_value['$gte']}
                    elif '$lt' in filter_value:
                        mongo_filter['rating'] = {'$lte': filter_value['$lt']}
                    elif '$lte' in filter_value:
                        mongo_filter['rating'] = {'$lte': filter_value['$lte']}
                    elif '$eq' in filter_value:
                        mongo_filter['rating'] = filter_value['$eq']
                    else:
                        # If it's some other dict format, just use it as-is
                        mongo_filter['rating'] = filter_value
                else:
                    # IMPROVED: Direct value - handle exact matches
                    mongo_filter['rating'] = filter_value
            elif filter_name == 'director':
                # Map to 'directors' field (plural)
                mongo_filter['directors'] = {'$regex': re.escape(filter_value), '$options': 'i'}
            elif filter_name == 'actor':
                # Map to 'actors' field (plural)
                mongo_filter['actors'] = {'$regex': re.escape(filter_value), '$options': 'i'}
        
        return mongo_filter
    
    def _build_aggregate_query(self, details: Dict, mongo_filter: Dict, collection: str) -> Dict[str, Any]:
        """Build MongoDB aggregation query"""
        
        agg_function = details.get('aggregate_function', 'avg')
        agg_field = details.get('aggregate_field', 'rating')
        
        pipeline = []
        
        # Add match stage if there are filters
        if mongo_filter:
            pipeline.append({'$match': mongo_filter})
        
        # Add group stage
        group_stage = {'_id': None}
        
        if agg_function == 'avg':
            group_stage[f'average_{agg_field}'] = {'$avg': f'${agg_field}'}
        elif agg_function == 'sum':
            group_stage[f'total_{agg_field}'] = {'$sum': f'${agg_field}'}
        elif agg_function == 'max':
            group_stage[f'max_{agg_field}'] = {'$max': f'${agg_field}'}
        elif agg_function == 'min':
            group_stage[f'min_{agg_field}'] = {'$min': f'${agg_field}'}
        
        pipeline.append({'$group': group_stage})
        pipeline.append({'$project': {'_id': 0}})
        
        return {
            'collection': collection,
            'operation': 'aggregate',
            'pipeline': pipeline
        }
    
    def _get_projection(self, entity: str) -> Dict[str, int]:
        """Get appropriate projection for entity with correct field names"""
        
        if entity == 'MOVIE':
            return {
                'title': 1, 
                'year': 1, 
                'rating': 1, 
                'genres': 1,  # Use 'genres' (plural)
                'directors': 1,
                'runtime': 1,  # Use 'runtime' not 'runtime_minutes'
                'revenue': 1   # Use 'revenue' not 'revenue_millions'
            }
        elif entity == 'GENRE':
            return {'name': 1, 'description': 1, 'movie_count': 1}
        else:
            return {}

    # FIXED: Rule-based fallback methods with improved DELETE detection
    def _determine_operation_rules(self, text: str) -> str:
        """Determine operation using improved rule-based approach - FIXED DELETE priority"""
        text_lower = text.lower().strip()
        
        # Check for DELETE first (highest priority for this issue)
        if re.search(r'\b(delete|remove|drop|eliminate|destroy)\b', text_lower):
            return 'DELETE'
        
        # Check for CREATE
        if re.search(r'\b(add|create|insert|new|make)\b', text_lower):
            return 'CREATE'
        
        # Check for UPDATE  
        if re.search(r'\b(update|modify|change|edit|set|alter)\b', text_lower):
            return 'UPDATE'
        
        # Check for COUNT
        if re.search(r'\b(count|how many|number of)\b', text_lower):
            return 'COUNT'
        
        # Check for AGGREGATE
        if re.search(r'\b(average|mean|sum|total|max|min|highest|lowest)\b', text_lower):
            return 'AGGREGATE'
        
        # Default to READ for everything else
        return 'READ'
    
    def _determine_entity_rules(self, text: str) -> str:
        """Determine entity using rule-based approach"""
        for entity, pattern in self.entity_patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                return entity
        return 'MOVIE'
    
    def _extract_filters_rules(self, text: str) -> Dict[str, Any]:
        """Extract filters using rule-based approach - FIXED rating extraction"""
        filters = {}
        
        # Extract genre
        genre_match = re.search(r'\b(action|comedy|drama|thriller|horror|sci-fi|romance|fantasy|adventure|crime|documentary|animation|biography|history|mystery|war|western|musical|sport)\b', text, re.IGNORECASE)
        if genre_match:
            filters['genre'] = genre_match.group().title()
        
        # Extract year
        year_match = re.search(r'\b(19|20)\d{2}\b', text)
        if year_match:
            filters['year'] = int(year_match.group())
        
        # FIXED: Enhanced rating extraction
        rating_patterns = [
            # Comparative operators
            (r'rating\s*(?:is\s+)?(?:above|over|greater\s+than|>|>=)\s*(\d+\.?\d*)', 'above'),
            (r'rating\s*(?:is\s+)?(?:below|under|less\s+than|<|<=)\s*(\d+\.?\d*)', 'below'),
            
            # Exact matches - multiple patterns
            (r'with\s+rating\s+(\d+\.?\d*)', 'exact'),
            (r'rating\s+(?:is\s+|equals?\s+|=\s*)?(\d+\.?\d*)', 'exact'),
            (r'rated\s+(\d+\.?\d*)', 'exact'),
            (r'(?:^|\s)(\d+\.?\d*)\s+rating', 'exact'),  # "8.1 rating"
            (r'rating:\s*(\d+\.?\d*)', 'exact'),  # "rating: 8.1"
        ]
        
        for pattern, operation_type in rating_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if operation_type == 'above':
                    filters['rating'] = {
                        'operator': 'above',
                        'value': float(match.group(1))
                    }
                elif operation_type == 'below':
                    filters['rating'] = {
                        'operator': 'below', 
                        'value': float(match.group(1))
                    }
                elif operation_type == 'exact':
                    filters['rating'] = float(match.group(1))
                break  # Use first match found
        
        # Extract title - more patterns
        title_patterns = [
            r'(?:called|named|titled|with name)\s+["\']([^"\']+)["\']',
            r'(?:called|named|titled|with name)\s+([A-Za-z0-9\s]+?)(?:\s|$)',
            r'movie\s+["\']([^"\']+)["\']',
            r'film\s+["\']([^"\']+)["\']'
        ]
        
        for pattern in title_patterns:
            title_match = re.search(pattern, text, re.IGNORECASE)
            if title_match:
                filters['title'] = title_match.group(1).strip()
                break
        
        # Extract director
        director_match = re.search(r'(?:directed\s+by|director\s*:?)\s+([A-Za-z\s]+?)(?:\s|$)', text, re.IGNORECASE)
        if director_match:
            filters['director'] = director_match.group(1).strip()
        
        # Extract actor
        actor_match = re.search(r'(?:starring|actor\s*:?|stars?)\s+([A-Za-z\s]+?)(?:\s|$)', text, re.IGNORECASE)
        if actor_match:
            filters['actor'] = actor_match.group(1).strip()
        
        return filters
    
    def _execute_mongodb_query(self, query_info: Dict[str, Any]) -> Any:
        """Execute MongoDB query and return results - FIXED ObjectId serialization"""
        
        if not self.mongodb_connected or self.db is None:
            return {
                'error': 'MongoDB connection not available', 
                'simulated': True,
                'note': 'Please check your MongoDB connection string in .env file'
            }
        
        try:
            collection = self.db[query_info['collection']]
            operation = query_info['operation']
            
            print(f"Executing {operation} on collection {query_info['collection']}")
            
            if operation == 'find':
                cursor = collection.find(
                    query_info['filter'],
                    query_info.get('projection')
                )
                
                if 'limit' in query_info and query_info['limit']:
                    cursor = cursor.limit(query_info['limit'])
                
                results = list(cursor)
                
                # Convert ObjectIds to strings
                for result in results:
                    if '_id' in result:
                        result['_id'] = str(result['_id'])
                
                return {
                    'results': results,
                    'count': len(results),
                    'operation': operation
                }
                
            elif operation == 'aggregate':
                results = list(collection.aggregate(query_info['pipeline']))
                
                # Convert ObjectIds to strings in aggregation results
                for result in results:
                    if '_id' in result and result['_id'] is not None:
                        result['_id'] = str(result['_id'])
                
                return {
                    'results': results,
                    'operation': operation
                }
                
            elif operation == 'insert_one':
                result = collection.insert_one(query_info['document'])
                
                # FIXED: Don't include the document with ObjectId in response
                # Just return the converted ID and metadata
                return {
                    'inserted_id': str(result.inserted_id),
                    'acknowledged': result.acknowledged,
                    'operation': operation
                }
                
            elif operation == 'update_one':
                result = collection.update_one(
                    query_info['filter'],
                    query_info['update']
                )
                return {
                    'matched_count': result.matched_count,
                    'modified_count': result.modified_count,
                    'operation': operation
                }
                
            elif operation == 'delete_one':
                result = collection.delete_one(query_info['filter'])
                return {
                    'deleted_count': result.deleted_count,
                    'operation': operation
                }
                
            elif operation == 'count_documents':
                count = collection.count_documents(query_info['filter'])
                return {
                    'count': count,
                    'operation': operation
                }
                
            else:
                return {'error': f'Unsupported operation: {operation}'}
                
        except Exception as e:
            print(f"MongoDB execution error: {e}")
            return {'error': f'MongoDB execution failed: {str(e)}'}
    
    def compare_graphql_vs_mongodb(self, user_input: str) -> Dict[str, Any]:
        """Compare GraphQL and MongoDB approaches for the same query"""
        
        graphql_result = self.natural_language_to_graphql(user_input)
        mongodb_result = self.natural_language_to_mongodb(user_input)
        
        return {
            'user_input': user_input,
            'graphql_approach': graphql_result,
            'mongodb_approach': mongodb_result,
            'comparison': {
                'graphql_success': graphql_result.get('success', False),
                'mongodb_success': mongodb_result.get('success', False),
                'both_successful': graphql_result.get('success', False) and mongodb_result.get('success', False)
            }
        }
    
    def get_connection_status(self) -> Dict[str, Any]:
        """Get connection status information"""
        return {
            'mongodb_connected': self.mongodb_connected,
            'mongodb_uri_provided': self.mongodb_uri is not None,
            'database_name': 'imdb' if self.mongodb_connected else None,
            'ollama_available': self.ollama_available,
            'ollama_model': self.model_name
        }