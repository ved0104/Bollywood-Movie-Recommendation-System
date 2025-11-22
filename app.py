from flask import Flask, render_template, request, redirect, url_for
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import random
from pyspark import SparkConf
app = Flask(__name__)
spark = SparkSession.builder \
    .appName("MovieRecommenderReload") \
    .config("spark.ui.port", "40400") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .getOrCreate()

print("✅ Spark session started successfully")
# Load movies data
movies = spark.read.csv("hdfs://master:9000/spark_datasets/movielens/movies.csv", header=True, inferSchema=True)
movies_list = movies.collect()  # Small enough to collect for random sampling

# Load trained ALS model
model_path = "hdfs://master:9000/spark_datasets/movielens/als_model"
model = ALSModel.load(model_path)

@app.route('/')
def home():
    # Pick random 10 movies for rating
    sampled_movies = random.sample(movies_list, 10)
    return render_template('rate.html', movies=sampled_movies)

@app.route('/recommend', methods=['POST'])
def recommend():
    user_ratings = []
    user_id = random.randint(1000, 9999)  # temporary random user

    # Collect user ratings
    for key, value in request.form.items():
        if value:
            user_ratings.append((user_id, int(key), float(value)))

    if not user_ratings:
        return "No ratings provided. Please rate at least one movie."

    # Create Spark DataFrame for ratings
    ratings_df = spark.createDataFrame(user_ratings, ["userId", "movieId", "rating"])

    # Generate top 5 movie recommendations for this new user
    user_recs = model.recommendForUserSubset(ratings_df.select("userId").distinct(), 5)
    user_recs = user_recs.collect()[0]["recommendations"]

    # Extract movie IDs
    rec_movie_ids = [r["movieId"] for r in user_recs]
    rec_movies = movies.filter(movies.movieId.isin(rec_movie_ids)).collect()

    return render_template('recommend.html', rec_movies=rec_movies, user_id=user_id)

if __name__ == '__main__':
    app.run(debug=True)
