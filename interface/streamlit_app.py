import streamlit as st
from pymongo import MongoClient
import pandas as pd

# Configuration Streamlit
st.set_page_config(page_title="Recommandation de films", page_icon="🎬")
st.title("Bienvenue sur notre système de recommandation de films")

# Connexion à MongoDB
try:
    client = MongoClient("mongodb://localhost:27017")
    db = client["movie_lens"]

    movies_collection = db["movies"]
    ratings_collection = db["ratings"]

    # Vérification si la collection "movies" et "ratings" existe déjà
    if movies_collection.count_documents({}) == 0:
        st.warning("Aucun film trouvé dans la base de données.")
    if ratings_collection.count_documents({}) == 0:
        st.warning("Aucune évaluation trouvée dans la base de données.")

    # Sélection d'un utilisateur
    user_ids = ratings_collection.distinct("userId")
    selected_user = st.selectbox("Sélectionnez un utilisateur :", sorted(user_ids))

    # Récupérer les évaluations de l'utilisateur sélectionné
    user_ratings = list(ratings_collection.find({"userId": selected_user}))
    
    if user_ratings:
        # Récupérer les IDs des films que cet utilisateur a évalués
        movie_ids = [r["movieId"] for r in user_ratings]
        
        # Récupérer les films correspondants dans la collection movies
        movies = {m["movieId"]: m["title"] for m in movies_collection.find({"movieId": {"$in": movie_ids}})}

        # Créer un tableau des évaluations
        ratings_display = [
            {"Titre": movies.get(r["movieId"], "Inconnu"), "Note": r["rating"]}
            for r in user_ratings
        ]

        # Afficher les évaluations de l'utilisateur
        df = pd.DataFrame(ratings_display)
        st.subheader(f"🎬 Notes de l'utilisateur {selected_user} :")
        st.dataframe(df)

        # === Bloc Recommandations ===
        rec_db = client["movie_lens"]
        rec_collection = rec_db["recommendations"]
        movie_lens_movies = rec_db["movies"] 

        top_recs = list(
            rec_collection.find({"userId": selected_user})
            .sort("rating", -1)
            .limit(3)
        )

        if top_recs:
            rec_movie_ids = [r["movieId"] for r in top_recs]

            # Récupérer titre et genres des films recommandés
            rec_movies_data = {
                m["movieId"]: {"title": m["title"], "genres": m.get("genres", "N/A")}
                for m in movie_lens_movies.find({"movieId": {"$in": rec_movie_ids}})
            }

            # Préparer les recommandations à afficher
            recommendations_display = [
                {
                    "Titre recommandé": rec_movies_data.get(r["movieId"], {}).get("title", "Inconnu"),
                    "Genres": ", ".join(rec_movies_data.get(r["movieId"], {}).get("genres", "").split("|")),
                    "Score": round(r["rating"], 2),
                }
                for r in top_recs
            ]

            # Afficher les recommandations personnalisées
            st.subheader("📈 Recommandations personnalisées")
            st.dataframe(pd.DataFrame(recommendations_display))
        else:
            st.info("Aucune recommandation disponible pour cet utilisateur.")
    else:
        st.warning("Cet utilisateur n'a noté aucun film.")

except Exception as e:
    st.error(f"Erreur de connexion à MongoDB : {e}")
