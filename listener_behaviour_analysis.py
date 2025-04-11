from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, date_format, first, row_number,sum, lower, round, struct, max
from pyspark.sql.window import Window
from pyspark.sql.functions import lit

# Initialize Spark Session
spark = SparkSession.builder.appName("MusicStreamingAnalysis").getOrCreate()

# Load Data
logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Join logs with song metadata
enriched_logs = logs_df.join(songs_df, "song_id")

# 1. Find each user’s favorite genre
favorite_genre = enriched_logs.groupBy("user_id", "genre").agg(count("song_id").alias("play_count"))
window_spec = Window.partitionBy("user_id").orderBy(col("play_count").desc())
fav_genre = (favorite_genre
             .withColumn("rank", row_number().over(window_spec))
             .filter(col("rank") == 1)
             .select("user_id", "genre"))
fav_genre.write.csv("output/user_favorite_genres", header=True, mode="overwrite")

# 2. Average listen time per song
avg_listen_time = logs_df.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration"))
avg_listen_time.write.csv("output/avg_listen_time_per_song", header=True, mode="overwrite")

# 3. Top 10 most played songs this week
weekly_top_songs = enriched_logs.filter(
    date_format(col("timestamp"), "yyyy-MM-dd") >= "2025-03-17"
).groupBy("song_id", "title").agg(count("user_id").alias("play_count"))
top_10_songs = weekly_top_songs.orderBy(col("play_count").desc()).limit(10)
top_10_songs.write.csv("output/top_songs_this_week", header=True, mode="overwrite")

# 4. Recommend "Happy" songs to users who mostly listen to "Sad" songs
sad_listeners = (enriched_logs
                .filter(lower(col("mood")) == "sad")
                .groupBy("user_id")
                .agg(count("song_id").alias("sad_plays"))
                .filter(col("sad_plays") >= 2)  # At least 2 sad songs
                .select("user_id"))

happy_songs = songs_df.filter(lower(col("mood")) == "happy")
window_spec = Window.partitionBy("user_id").orderBy("song_id")

recommendations = (sad_listeners
                  .crossJoin(happy_songs)
                  .withColumn("rank", row_number().over(window_spec))
                  .filter(col("rank") <= 3)
                  .select("user_id", "song_id", "title", "artist", "mood"))

if recommendations.count() > 0:
    print(f"Generating {recommendations.count()} recommendations")
    recommendations.show(truncate=False)
    recommendations.write.csv("output/happy_recommendations", 
                            header=True, 
                            mode="overwrite")
else:
    print("No recommendations generated - check your filters")

# 5. Compute genre loyalty score

favorite_genre = enriched_logs.groupBy("user_id", "genre").agg(
    count("song_id").alias("play_count")
)

window_spec = Window.partitionBy("user_id").orderBy(col("play_count").desc())
fav_genre = (favorite_genre
             .withColumn("rank", row_number().over(window_spec))
             .filter(col("rank") == 1)
             .select("user_id", "genre", "play_count"))


user_total_plays = enriched_logs.groupBy("user_id").agg(
    count("song_id").alias("total_plays")
)

user_loyalty = fav_genre.join(user_total_plays, "user_id") \
    .withColumn("loyalty_score", col("play_count") / col("total_plays"))

loyal_users = user_loyalty.filter(col("loyalty_score") > 0.08)

if loyal_users.count() == 0:
    # If no users with loyalty score above 0.8, find the maximum loyalty score
    max_loyalty_score = user_loyalty.agg(max("loyalty_score")).collect()[0][0]
    message = f"No users with loyalty score above 0.8. The maximum loyalty score found is: {max_loyalty_score}"
    
    # Write the message to a CSV file
    message_df = spark.createDataFrame([(message,)], ["message"])
    message_df.write.csv("output/loyalty_message.csv", header=True, mode="overwrite")
else:
    # Save the result with users who have loyalty score above 0.8
    loyal_users.write.csv("output/genre_loyalty_scores", header=True, mode="overwrite")

# 6. Identify night owl users
night_owls = enriched_logs.filter(
    (col("timestamp").substr(12, 2).cast("int") >= 0) &
    (col("timestamp").substr(12, 2).cast("int") < 5)
).select("user_id").distinct()
night_owls.write.csv("output/night_owl_users", header=True, mode="overwrite")

# 7. Save enriched logs
enriched_logs.write.csv("output/enriched_logs", header=True, mode="overwrite")

print("Analysis complete!")