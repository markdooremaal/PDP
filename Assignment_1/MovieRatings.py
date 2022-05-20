from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieRatings(MRJob):

	# Define the steps MRJob will execute
	def steps(self):
		return [
			MRStep(mapper=self.get_movies, combiner=self.combine_movies, reducer=self.count_ratings),
			MRStep(reducer=self.sort_by_rating)
		]

	# Get all the movies from provided file and yield movieID
	def get_movies(self, _, line):
		(userID, movieID, rating, timestamp) = line.split('\t')
		yield movieID, 1
	
	# Combine rating and movieID
	def combine_movies(self, rating, counts):
		yield rating, sum(counts)
	
	# Count the ratings per movieID
	def count_ratings(self, key, values):
		yield None, (sum(values), key)
	
	# Sort movieID by rating
	def sort_by_rating(self, _, rating_counts):
		for count, key in sorted(rating_counts, reverse=True):
			yield (key, int(count))

# Start MRJob
if __name__ == '__main__':
    MovieRatings.run()