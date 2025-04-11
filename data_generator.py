import csv
import random
from faker import Faker

fake = Faker()

GENRES = ["Pop", "Rock", "Jazz", "HipHop", "Classical"]
MOODS = ["Happy", "Sad", "Energetic", "Chill"]

NUM_USERS = 100
NUM_SONGS = 50
NUM_LOGS = 1000

def generate_songs_metadata(output_file="songs_metadata.csv"):
    """
    Generates song metadata:
    song_id, title, artist, genre, mood
    """
    fieldnames = ["song_id", "title", "artist", "genre", "mood"]

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, NUM_SONGS + 1):
            song_id = f"S{i:04d}"
            title = fake.sentence(nb_words=3).rstrip(".")
            artist = fake.name()
            genre = random.choice(GENRES)
            mood = random.choice(MOODS)

            writer.writerow({
                "song_id": song_id,
                "title": title,
                "artist": artist,
                "genre": genre,
                "mood": mood
            })

def generate_listening_logs(output_file="listening_logs.csv"):
    """
    Generates listening logs:
    user_id, song_id, timestamp, duration_sec
    """
    fieldnames = ["user_id", "song_id", "timestamp", "duration_sec"]
    users = [f"U{uid:03d}" for uid in range(1, NUM_USERS + 1)]
    songs = [f"S{i:04d}" for i in range(1, NUM_SONGS + 1)]

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(NUM_LOGS):
            user_id = random.choice(users)
            song_id = random.choice(songs)
            timestamp = fake.date_time_between(start_date="-14d", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
            duration = random.randint(30, 300)

            writer.writerow({
                "user_id": user_id,
                "song_id": song_id,
                "timestamp": timestamp,
                "duration_sec": duration
            })

if __name__ == "__main__":
    generate_songs_metadata()
    generate_listening_logs()
    print("✅ songs_metadata.csv and listening_logs.csv generated.")
