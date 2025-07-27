# schema.py - Updated with consistent field names
import graphene
from graphene_mongo import MongoengineObjectType, MongoengineConnectionField
from models import Movie, Genre
from mongoengine import Q

class MovieType(MongoengineObjectType):
    class Meta:
        model = Movie
        interfaces = (graphene.relay.Node,)
    
    # Add backward compatibility field for existing queries
    genre = graphene.List(graphene.String)
    
    def resolve_genre(self, info):
        """Backward compatibility - map genres to genre for old queries"""
        return self.genres or []

class GenreType(MongoengineObjectType):
    class Meta:
        model = Genre
        interfaces = (graphene.relay.Node,)

class Query(graphene.ObjectType):
    node = graphene.relay.Node.Field()
    
    # Movie queries
    all_movies = MongoengineConnectionField(MovieType)
    movie_by_id = graphene.Field(MovieType, id=graphene.String())
    movies_by_genre = graphene.List(MovieType, genre=graphene.String())
    movies_by_year = graphene.List(MovieType, year=graphene.Int())
    movies_by_rating = graphene.List(MovieType, min_rating=graphene.Float())
    
    # Genre queries
    all_genres = MongoengineConnectionField(GenreType)
    genre_by_name = graphene.Field(GenreType, name=graphene.String())
    
    # Resolvers
    def resolve_all_movies(self, info, **kwargs):
        return Movie.objects.all()
    
    def resolve_all_genres(self, info, **kwargs):
        return Genre.objects.all()
    
    def resolve_movie_by_id(self, info, id):
        return Movie.objects(id=id).first()
    
    def resolve_movies_by_genre(self, info, genre):
        return Movie.objects(genres__icontains=genre)
    
    def resolve_movies_by_year(self, info, year):
        return Movie.objects(year=year)
    
    def resolve_movies_by_rating(self, info, min_rating):
        return Movie.objects(rating__gte=min_rating)
    
    def resolve_genre_by_name(self, info, name):
        return Genre.objects(name=name).first()

class CreateMovie(graphene.Mutation):
    class Arguments:
        title = graphene.String(required=True)
        genres = graphene.List(graphene.String)  # FIXED: Use 'genres' consistently
        description = graphene.String()
        directors = graphene.List(graphene.String)
        actors = graphene.List(graphene.String)
        year = graphene.Int()
        runtime = graphene.Int()  # FIXED: Use 'runtime' not 'runtime_minutes'
        rating = graphene.Float()
        votes = graphene.Int()
        revenue = graphene.Float()  # FIXED: Use 'revenue' not 'revenue_millions'
    
    movie = graphene.Field(lambda: MovieType)
    
    def mutate(self, info, title, **kwargs):
        # No field mapping needed - arguments match database fields
        movie = Movie(title=title, **kwargs)
        movie.save()
        return CreateMovie(movie=movie)

class UpdateMovie(graphene.Mutation):
    class Arguments:
        id = graphene.String(required=True)
        title = graphene.String()
        genres = graphene.List(graphene.String)  # FIXED: Use 'genres' consistently
        description = graphene.String()
        directors = graphene.List(graphene.String)
        actors = graphene.List(graphene.String)
        year = graphene.Int()
        runtime = graphene.Int()  # FIXED: Use 'runtime' not 'runtime_minutes'
        rating = graphene.Float()
        votes = graphene.Int()
        revenue = graphene.Float()  # FIXED: Use 'revenue' not 'revenue_millions'
    
    movie = graphene.Field(lambda: MovieType)
    
    def mutate(self, info, id, **kwargs):
        movie = Movie.objects(id=id).first()
        if movie:
            # No field mapping needed - arguments match database fields
            for key, value in kwargs.items():
                if value is not None:
                    setattr(movie, key, value)
            movie.save()
        return UpdateMovie(movie=movie)

class DeleteMovie(graphene.Mutation):
    class Arguments:
        id = graphene.String(required=True)
    
    success = graphene.Boolean()
    
    def mutate(self, info, id):
        movie = Movie.objects(id=id).first()
        if movie:
            movie.delete()
            return DeleteMovie(success=True)
        return DeleteMovie(success=False)

class CreateGenre(graphene.Mutation):
    class Arguments:
        name = graphene.String(required=True)
        description = graphene.String()
    
    genre = graphene.Field(lambda: GenreType)
    
    def mutate(self, info, name, description=None):
        genre = Genre(name=name, description=description)
        genre.save()
        return CreateGenre(genre=genre)

class Mutation(graphene.ObjectType):
    create_movie = CreateMovie.Field()
    update_movie = UpdateMovie.Field()
    delete_movie = DeleteMovie.Field()
    create_genre = CreateGenre.Field()

schema = graphene.Schema(query=Query, mutation=Mutation)