from flask import Flask, render_template, request
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col, lit, when, rand
import random

# ----------------------------
# Initialize Flask + Spark
# ----------------------------
app = Flask(__name__)

spark = (
    SparkSession.builder
    .appName("BollywoodHybridRecommender")
    .master("spark://master:7077")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.ui.port", "4050")
    .config("spark.executor.memory", "3g")
    .config("spark.driver.memory", "3g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "200s")
    .config("spark.shuffle.io.maxRetries", "10")
    .config("spark.shuffle.io.retryWait", "30s")
    .getOrCreate()
)
print("✅ Spark session started successfully")

# ----------------------------
# Load datasets
# ----------------------------
movies = spark.read.parquet("hdfs://master:9000/spark_datasets/bolly_data/bolly_merged.parquet")

# Fill missing posters
movies = movies.withColumn(
    "poster_path",
    when(
        (col("poster_path").isNull()) | (col("poster_path") == ""),
        lit("https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg")
    ).otherwise(col("poster_path"))
)
print("✅ Bollywood dataset loaded successfully")

# ----------------------------
# Load ALS model and mapping
# ----------------------------
model_path = "hdfs://master:9000/spark_datasets/bolly_data/als_bollywood_model"
model = ALSModel.load(model_path)
print("✅ ALS model loaded successfully")

movie_mapping = spark.read.parquet("hdfs://master:9000/spark_datasets/bolly_data/movie_mapping")
print("✅ Movie-ID mapping loaded successfully")

# ----------------------------
# Routes
# ----------------------------
@app.route('/')
def index():
    """Render random movies for the user to rate."""
    movies_sample = (
        movies.select("imdb_id", "title", "poster_path", "genres")
        .orderBy(rand())
        .limit(10)
        .collect()
    )
    movies_sample = [row.asDict() for row in movies_sample]
    return render_template('rate_bolly.html', movies=movies_sample)


@app.route('/recommend', methods=['POST'])
def recommend():
    user_ratings = []
#    user_id = random.randint(1, 1000)
    min_user, max_user = model.userFactors.selectExpr("min(id)", "max(id)").first()
    user_id = random.randint(min_user, max_user)
    # Collect user ratings from UI
    for key, value in request.form.items():
        if value:
            user_ratings.append((user_id, key.strip(), float(value)))

    if not user_ratings:
        return "⚠️ Please rate at least one movie."

    from pyspark.sql.functions import lower, trim, col, explode, rand
    ratings_df = spark.createDataFrame(user_ratings, ["userId", "imdb_id", "rating"]) \
        .withColumn("imdb_id", trim(lower(col("imdb_id"))))

    mapping_clean = movie_mapping.withColumn("imdb_id", trim(lower(col("imdb_id"))))

    # Join with mapping
    ratings_with_ids = ratings_df.join(mapping_clean, "imdb_id", "inner")

    if ratings_with_ids.count() == 0:
        print("⚠️ No matching movie IDs found in mapping.")
        top_movies = movies.orderBy(col("imdb_rating").desc()).limit(5).collect()
        return render_template("recommend_bolly.html", rec_movies=top_movies, user_id=user_id)

    als_input = ratings_with_ids.select("userId", "movieId", "rating")
    model.setColdStartStrategy("drop")

    print("\n🟢 User Ratings Submitted:")
    als_input.show()

    try:
        # Detect ALS recommendation field
        rec_schema = model.recommendForAllUsers(1).schema["recommendations"].dataType.elementType
        available_fields = [f.name for f in rec_schema.fields]
        field_name = "movieIndex" if "movieIndex" in available_fields else "movieId"
        print(f"🧠 ALS uses field: {field_name}")

        # Try generating recommendations
        user_recs = model.recommendForUserSubset(als_input.select("userId").distinct(), 10)

        if user_recs.count() == 0:
            print("⚠️ No direct ALS recs — cold-start fallback triggered.")
            raise Exception("cold_start")

        # Flatten recommendations
        user_recs = (
            user_recs
            .select("userId", explode("recommendations").alias("rec"))
            .select(
                "userId",
                col(f"rec.{field_name}").alias("movieId"),
                col("rec.rating").alias("pred_rating")
            )
        )

        if user_recs.count() == 0:
            raise Exception("ALS output empty after flattening")

        rec_ids = [row["movieId"] for row in user_recs.orderBy(rand()).limit(5).collect()]
        print(f"🎯 Recommended numeric movie IDs: {rec_ids}")

    except Exception as e:
        print(f"⚠️ Recommendation generation failed: {e}")

        # === Cold-start fallback: Recommend popular + genre-similar movies ===
        print("🟡 Using fallback recommender (top IMDb + random genre match)")

        # Get user's favorite genres (from movies they rated)
        fav_genres = (
            movies.join(mapping_clean, "imdb_id", "inner")
            .filter(col("movieId").isin([r.movieId for r in als_input.collect()]))
            .select("genres").rdd.flatMap(lambda x: x).collect()
        )

        if fav_genres:
            genre_word = fav_genres[0].split(",")[0]
            rec_movies = (
                movies.filter(col("genres").contains(genre_word))
                .orderBy(col("imdb_rating").desc())
                .limit(5).collect()
            )
        else:
            rec_movies = movies.orderBy(col("imdb_rating").desc()).limit(5).collect()

        return render_template("recommend_bolly.html", rec_movies=rec_movies, user_id=user_id)

    # === Normal ALS-based recommendation flow ===
    rec_movies = (
        movies.join(mapping_clean, "imdb_id", "inner")
        .filter(col("movieId").isin(rec_ids))
        .collect()
    )

    if not rec_movies:
        print("⚠️ Empty rec_movies fallback triggered.")
        rec_movies = movies.orderBy(col("imdb_rating").desc()).limit(5).collect()

    print(f"🎬 Returning {len(rec_movies)} recommendations for user {user_id}")
    return render_template("recommend_bolly.html", rec_movies=rec_movies, user_id=user_id)



# ----------------------------
# Run Flask app
# ----------------------------
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
