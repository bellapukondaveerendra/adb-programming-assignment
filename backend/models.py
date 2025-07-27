
from mongoengine import Document, StringField, ListField, IntField, FloatField

class Movie(Document):
    meta = {'collection': 'movies'}
    
    # EXACT field names from your database - no extras, no different names
    ids = IntField()
    title = StringField(required=True, max_length=200)
    genres = ListField(StringField(max_length=50))
    description = StringField()
    directors = ListField(StringField(max_length=100))
    actors = ListField(StringField(max_length=100))
    year = IntField()
    runtime = IntField()  # Note: can be None in database
    rating = FloatField()
    votes = IntField()
    revenue = FloatField()  # Note: can be None in database
    
    # Only add this property for GraphQL backward compatibility
    @property
    def genre(self):
        """GraphQL backward compatibility - return genres as genre"""
        return self.genres or []

class Genre(Document):
    meta = {'collection': 'genres'}
    
    name = StringField(required=True, unique=True, max_length=50)
    description = StringField()
    movie_count = IntField(default=0)