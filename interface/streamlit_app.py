import streamlit as st
from pymongo import MongoClient
import pandas as pd

# Configuration Streamlit
st.set_page_config(page_title="Recommandation de films", page_icon="🎬")

st.title("Bienvenue sur notre système de recommandation de films")

# Connexion à MongoDB
try:
    client = MongoClient("mongodb://localhost:27017")
    db = client["recommandation_films"]

    movies_collection = db["movies"]
    ratings_collection = db["ratings"]

    #Si la base est vide
    if movies_collection.count_documents({}) == 0:
        movies_collection.insert_many([
            {"movieId": 1, "title": "Inception", "genre": "Sci-Fi"},
            {"movieId": 2, "title": "The Matrix", "genre": "Action"},
            {"movieId": 3, "title": "Interstellar", "genre": "Sci-Fi"},
            {"movieId": 4, "title": "Parasite", "genre": "Drama"},
        ])
        st.info("Films de test insérés.")

    if ratings_collection.count_documents({}) == 0:
        ratings_collection.insert_many([
            {"userId": 1, "movieId": 1, "rating": 5},
            {"userId": 1, "movieId": 2, "rating": 4.5},
            {"userId": 2, "movieId": 3, "rating": 4},
            {"userId": 2, "movieId": 1, "rating": 4},
            {"userId": 3, "movieId": 4, "rating": 4.5},
        ])
        st.info("Évaluations de test insérées.")

    user_ids = ratings_collection.distinct("userId")
    selected_user = st.selectbox("Sélectionnez un utilisateur :", sorted(user_ids))

    user_ratings = list(ratings_collection.find({"userId": selected_user}))
    if user_ratings:
        movie_ids = [r["movieId"] for r in user_ratings]
        movies = {m["movieId"]: m["title"] for m in movies_collection.find({"movieId": {"$in": movie_ids}})}

        ratings_display = [
            {"Titre": movies.get(r["movieId"], "Inconnu"), "Note": r["rating"]}
            for r in user_ratings
        ]

        df = pd.DataFrame(ratings_display)
        st.subheader(f"🎬 Notes de l'utilisateur {selected_user} :")
        st.dataframe(df)
    else:
        st.warning("Cet utilisateur n'a noté aucun film.")

except Exception as e:
    st.error(f"Erreur de connexion à MongoDB : {e}")
