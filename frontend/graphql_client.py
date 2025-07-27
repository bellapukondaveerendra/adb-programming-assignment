import requests
import json
from typing import Dict, Any

class GraphQLClient:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
    
    def execute_query(self, query: str, variables: Dict = None) -> Dict[str, Any]:
        """Execute a GraphQL query"""
        payload = {
            'query': query,
            'variables': variables or {}
        }
        
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            return response.json()
        except Exception as e:
            return {'errors': [str(e)]}
    
    def natural_language_query(self, user_input: str) -> Dict[str, Any]:
        """Send natural language input to be processed"""
        try:
            response = requests.post(
                f"{self.endpoint.replace('/graphql', '')}/natural-language",
                json={'input': user_input},
                headers={'Content-Type': 'application/json'}
            )
            return response.json()
        except Exception as e:
            return {'error': str(e)}