# streamlit_app.py - Enhanced version with MongoDB query support
import streamlit as st
import pandas as pd
from graphql_client import GraphQLClient
import json
import requests

# Configuration
BACKEND_URL = "http://localhost:5000"
client = GraphQLClient(f"{BACKEND_URL}/graphql")

st.set_page_config(
    page_title="IMDB Movie Database - GraphQL & MongoDB CRUD with LLM",
    page_icon="🎬",
    layout="wide"
)

st.title("🎬 IMDB Movie Database")
# st.subtitle("GraphQL & MongoDB CRUD Operations with Natural Language Processing")

# Sidebar for operation selection
st.sidebar.title("Operations")
operation_type = st.sidebar.selectbox(
    "Choose Operation Type",
    ["Natural Language"]
)

def make_request(endpoint, data):
    """Helper function to make API requests"""
    try:
        response = requests.post(
            f"{BACKEND_URL}/{endpoint}",
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        return response.json()
    except Exception as e:
        return {'error': str(e)}

def display_results(result, title="Results"):
    """Helper function to display results in a formatted way"""
    st.subheader(title)
    
    if isinstance(result, dict):
        # Handle different result structures
        if 'data' in result and result['data']:
            data = result['data']
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        st.write(f"**{key.replace('_', ' ').title()}:**")
                        df = pd.json_normalize(value)
                        st.dataframe(df, use_container_width=True)
                    elif isinstance(value, dict) and 'edges' in value:
                        # GraphQL relay format
                        items = [edge['node'] for edge in value['edges']]
                        if items:
                            st.write(f"**{key.replace('_', ' ').title()}:**")
                            df = pd.json_normalize(items)
                            st.dataframe(df, use_container_width=True)
                    elif isinstance(value, dict):
                        st.write(f"**{key.replace('_', ' ').title()}:**")
                        st.json(value)
                    else:
                        st.write(f"**{key}:** {value}")
            else:
                st.json(data)
        elif 'query_result' in result and result['query_result']:
            # MongoDB result format
            query_result = result['query_result']
            if 'results' in query_result:
                if query_result['results']:
                    df = pd.json_normalize(query_result['results'])
                    st.dataframe(df, use_container_width=True)
                    st.write(f"Found {len(query_result['results'])} results")
                else:
                    st.info("No results found")
            elif 'count' in query_result:
                st.metric("Total Count", query_result['count'])
            else:
                st.json(query_result)
        else:
            st.json(result)
    else:
        st.json(result)

if operation_type == "Natural Language (GraphQL)":
    st.header("🤖 Natural Language Interface - GraphQL")
    st.write("Ask questions in plain English and get GraphQL responses!")
    
    # Example queries
    st.write("**Example queries:**")
    examples = [
        "Show me all action movies",
        "Find movies from 2020",
        "Show all genres"
    ]
    
    for example in examples:
        if st.button(f"📝 {example}", key=f"graphql_example_{example}"):
            st.session_state.user_input_graphql = example
    
    # User input
    user_input = st.text_area(
        "Enter your request:",
        value=st.session_state.get('user_input_graphql', ''),
        height=100,
        placeholder="e.g., 'Show me all movies from 2020 with rating above 7.0'"
    )
    
    if st.button("🚀 Process with GraphQL", type="primary"):
        if user_input:
            with st.spinner("Processing your request with GraphQL..."):
                result = make_request('natural-language-graphql', {'input': user_input})
                
                if 'error' in result:
                    st.error(f"Error at 120: {result['error']}")
                else:
                    # Show generated GraphQL
                    st.subheader("Generated GraphQL Query")
                    st.code(result.get('generated_query', ''), language='graphql')
                    
                    # Show results
                    display_results(result)
                    
                    # Show errors if any
                    if result.get('errors'):
                        st.subheader("Errors")
                        for error in result['errors']:
                            st.error(error)

elif operation_type == "Natural Language":
    st.header("🍃 Natural Language Interface - MongoDB")
    st.write("Ask questions in plain English and get direct MongoDB responses!")
    
    # Example queries
    st.write("**Example queries:**")
    examples = [
        "Show me all action movies",
        "Find movies from 2019",
        "Get movies with rating above 8.0",
    ]
    
    for example in examples:
        if st.button(f"📝 {example}", key=f"mongodb_example_{example}"):
            st.session_state.user_input_mongodb = example
    
    # User input
    user_input = st.text_area(
        "Enter your request:",
        value=st.session_state.get('user_input_mongodb', ''),
        height=100,
        placeholder="e.g., 'Count all action movies'"
    )
    
    if st.button("🚀 Process with MongoDB", type="primary"):
        if user_input:
            with st.spinner("Processing your request with MongoDB..."):
                result = make_request('natural-language-mongodb', {'input': user_input})
                
                if 'error' in result:
                    st.error(f"Error at 169: {result['error']}")
                else:
                    # Show generated MongoDB query
                    st.subheader("Generated MongoDB Query")
                    st.code(json.dumps(result.get('mongodb_query', {}), indent=2), language='json')
                    
                    # Show results
                    display_results(result, "MongoDB Query Results")

elif operation_type == "Compare Both Approaches":
    st.header("⚖️ Compare GraphQL vs MongoDB")
    st.write("See how the same natural language query is handled by both GraphQL and MongoDB!")
    
    # Example queries
    st.write("**Try these comparisons:**")
    examples = [
        "Show me all action movies",
        "Find movies from 2019",
        "Get movies with rating above 8.0",
        "Count how many movies we have",
        "Show all genres"
    ]
    
    for example in examples:
        if st.button(f"📝 {example}", key=f"compare_example_{example}"):
            st.session_state.user_input_compare = example
    
    # User input
    user_input = st.text_area(
        "Enter your request:",
        value=st.session_state.get('user_input_compare', ''),
        height=100,
        placeholder="e.g., 'Show me all action movies'"
    )
    
    if st.button("🔍 Compare Both Approaches", type="primary"):
        if user_input:
            with st.spinner("Comparing GraphQL vs MongoDB approaches..."):
                result = make_request('natural-language-compare', {'input': user_input})
                
                if 'error' in result:
                    st.error(f"Error at 210: {result['error']}")
                else:
                    # Create two columns for comparison
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("🔧 GraphQL Approach")
                        graphql_result = result.get('graphql_approach', {})
                        
                        if graphql_result.get('success'):
                            st.success("✅ GraphQL query generated successfully")
                            st.code(graphql_result.get('graphql_query', ''), language='graphql')
                            
                            if 'execution_result' in graphql_result:
                                display_results(graphql_result['execution_result'], "GraphQL Results")
                        else:
                            st.error(f"❌ GraphQL failed: {graphql_result.get('error', 'Unknown error')}")
                    
                    with col2:
                        st.subheader("🍃 MongoDB Approach")
                        mongodb_result = result.get('mongodb_approach', {})
                        
                        if mongodb_result.get('success'):
                            st.success("✅ MongoDB query generated successfully")
                            st.code(json.dumps(mongodb_result.get('mongodb_query', {}), indent=2), language='json')
                            
                            display_results(mongodb_result, "MongoDB Results")
                        else:
                            st.error(f"❌ MongoDB failed: {mongodb_result.get('error', 'Unknown error')}")
                    
                    # Comparison summary
                    st.subheader("📊 Comparison Summary")
                    comparison = result.get('comparison', {})
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("GraphQL Success", "✅" if comparison.get('graphql_success') else "❌")
                    with col2:
                        st.metric("MongoDB Success", "✅" if comparison.get('mongodb_success') else "❌")
                    with col3:
                        st.metric("Both Successful", "✅" if comparison.get('both_successful') else "❌")

elif operation_type == "Direct GraphQL":
    st.header("🔧 Direct GraphQL Interface")
    
    # Predefined queries
    st.subheader("Quick Queries")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("All Movies"):
            st.session_state.graphql_query = """
            query {
              allMovies {
                edges {
                  node {
                    title
                    year
                    rating
                    genre
                  }
                }
              }
            }
            """
    
    with col2:
        if st.button("All Genres"):
            st.session_state.graphql_query = """
            query {
              allGenres {
                edges {
                  node {
                    name
                    description
                    movieCount
                  }
                }
              }
            }
            """
    
    with col3:
        if st.button("High Rated Movies"):
            st.session_state.graphql_query = """
            query {
              moviesByRating(minRating: 8.0) {
                title
                rating
                year
                genre
              }
            }
            """
    
    # Custom GraphQL input
    graphql_query = st.text_area(
        "GraphQL Query:",
        value=st.session_state.get('graphql_query', ''),
        height=200,
        placeholder="Enter your GraphQL query here..."
    )
    
    if st.button("Execute GraphQL Query"):
        if graphql_query:
            with st.spinner("Executing GraphQL query..."):
                result = client.execute_query(graphql_query)
                display_results(result)

elif operation_type == "Direct MongoDB":
    st.header("🍃 Direct MongoDB Interface")
    
    # Predefined queries
    st.subheader("Quick Queries")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("All Movies", key="mongo_all_movies"):
            st.session_state.mongodb_query = {
                "collection": "movies",
                "operation": "find",
                "filter": {},
                "projection": {"title": 1, "year": 1, "rating": 1, "genre": 1}
            }
    
    with col2:
        if st.button("Count Movies", key="mongo_count"):
            st.session_state.mongodb_query = {
                "collection": "movies",
                "operation": "count_documents",
                "filter": {}
            }
    
    with col3:
        if st.button("Average Rating", key="mongo_avg"):
            st.session_state.mongodb_query = {
                "collection": "movies",
                "operation": "aggregate",
                "pipeline": [
                    {"$group": {"_id": None, "average_rating": {"$avg": "$rating"}}},
                    {"$project": {"_id": 0, "average_rating": 1}}
                ]
            }
    
    # Custom MongoDB query input
    mongodb_query = st.text_area(
        "MongoDB Query (JSON format):",
        value=json.dumps(st.session_state.get('mongodb_query', {}), indent=2) if st.session_state.get('mongodb_query') else '',
        height=200,
        placeholder='{"collection": "movies", "operation": "find", "filter": {}, "projection": {"title": 1, "year": 1}}'
    )
    
    if st.button("Execute MongoDB Query"):
        if mongodb_query:
            try:
                query_dict = json.loads(mongodb_query)
                with st.spinner("Executing MongoDB query..."):
                    result = make_request('mongodb-query', query_dict)
                    
                    if 'error' in result:
                        st.error(f"Error at 370: {result['error']}")
                    else:
                        st.subheader("Query Executed")
                        st.code(json.dumps(result['query'], indent=2), language='json')
                        
                        st.subheader("Results")
                        query_result = result['result']
                        
                        if 'results' in query_result:
                            if query_result['results']:
                                df = pd.json_normalize(query_result['results'])
                                st.dataframe(df, use_container_width=True)
                                st.write(f"Found {len(query_result['results'])} results")
                            else:
                                st.info("No results found")
                        elif 'count' in query_result:
                            st.metric("Count", query_result['count'])
                        else:
                            st.json(query_result)
            
            except json.JSONDecodeError:
                st.error("Invalid JSON format. Please check your query syntax.")

elif operation_type == "Browse Movies":
    st.header("🎭 Browse Movies")
    
    # Filters
    col1, col2 = st.columns(2)
    
    with col1:
        year_filter = st.selectbox(
            "Filter by Year",
            options=["All"] + list(range(2016, 2021)),
            index=0
        )
    
    with col2:
        rating_filter = st.slider(
            "Minimum Rating",
            min_value=1.0,
            max_value=10.0,
            value=1.0,
            step=0.1
        )
    
    # Build query based on filters
    if year_filter != "All":
        query = f"""
        query {{
          moviesByYear(year: {year_filter}) {{
            title
            year
            rating
            genre
            directors
            runtimeMinutes
          }}
        }}
        """
    else:
        query = f"""
        query {{
          moviesByRating(minRating: {rating_filter}) {{
            title
            year
            rating
            genre
            directors
            runtimeMinutes
          }}
        }}
        """
    
    if st.button("Load Movies"):
        with st.spinner("Loading movies..."):
            result = client.execute_query(query)
            display_results(result)

elif operation_type == "Manage Genres":
    st.header("🏷️ Manage Genres")
    
    # Add new genre
    st.subheader("Add New Genre")
    with st.form("add_genre"):
        genre_name = st.text_input("Genre Name")
        genre_description = st.text_area("Description (optional)")
        
        if st.form_submit_button("Add Genre"):
            mutation = f"""
            mutation {{
              createGenre(name: "{genre_name}", description: "{genre_description}") {{
                genre {{
                  name
                  description
                }}
              }}
            }}
            """
            
            result = client.execute_query(mutation)
            if result.get('data'):
                st.success("Genre added successfully!")
            if result.get('errors'):
                for error in result['errors']:
                    st.error(error)
    
    # List all genres
    st.subheader("All Genres")
    if st.button("Load Genres"):
        query = """
        query {
          allGenres {
            edges {
              node {
                name
                description
                movieCount
              }
            }
          }
        }
        """
        
        result = client.execute_query(query)
        display_results(result)

# Sidebar info
st.sidebar.markdown("---")
st.sidebar.markdown("### 🔧 Available Endpoints")
st.sidebar.markdown("- **GraphQL**: /graphql")
st.sidebar.markdown("- **MongoDB**: /mongodb-query")
st.sidebar.markdown("- **Natural Language**: /natural-language-*")

# Footer
st.markdown("---")
st.markdown("**Tech Stack:** Python Flask + GraphQL + MongoDB Atlas + Streamlit + Rule-based NLP")

# Add debugging info
if st.sidebar.checkbox("Show Debug Info"):
    st.sidebar.markdown("### 🐛 Debug Information")
    
    # Test backend connection
    try:
        health_response = requests.get(f"{BACKEND_URL}/health")
        if health_response.status_code == 200:
            health_data = health_response.json()
            st.sidebar.success("✅ Backend Connected")
            st.sidebar.json(health_data)
        else:
            st.sidebar.error("❌ Backend Connection Failed")
    except Exception as e:
        st.sidebar.error(f"❌ Backend Error: {e}")