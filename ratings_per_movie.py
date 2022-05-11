from mrjob.job import MRJob
from mrjob.step import MRStep

## :TODO Add comments explaining the code

class RatingsPerMovie (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings_per_movie,
                   reducer=self.reducer_count_ratings_per_movie)
        ]

    def mapper_get_ratings_per_movie(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings_per_movie (self, movieID, rating):
        yield (movieID, sum(rating))

if __name__ == '__main__':
    RatingsPerMovie.run()