import streamlit as st
from pymongo import MongoClient
import pandas as pd
import altair as alt

# Configuration Streamlit
st.set_page_config(page_title="Recommandation de films", page_icon="🎬")
st.title("Bienvenue sur notre système de recommandation de films")

# Connexion à MongoDB
try:
    client = MongoClient("mongodb://localhost:27017")
    db = client["movie_lens"]

    movies_collection = db["movies"]
    ratings_collection = db["ratings"]

    # Vérification si les collections existent et ne sont pas vides
    if movies_collection.count_documents({}) == 0:
        st.warning("Aucun film trouvé dans la base de données.")
    if ratings_collection.count_documents({}) == 0:
        st.warning("Aucune évaluation trouvée dans la base de données.")

    # Champ de saisie pour sélectionner un utilisateur
    selected_user = st.number_input(
        "Entrez l'ID utilisateur (entre 1 et 1000) :", min_value=1, max_value=1000, step=1
    )

    # Vérifier que l'utilisateur existe dans la base
    user_exists = ratings_collection.find_one({"userId": selected_user})

    if user_exists:
        # Récupérer les évaluations de l'utilisateur sélectionné
        user_ratings = list(ratings_collection.find({"userId": selected_user}))

        # Récupérer les IDs des films évalués
        movie_ids = [r["movieId"] for r in user_ratings]

        # Récupérer les films correspondants avec titres et genres
        movies_data = {
            m["movieId"]: {"title": m["title"], "genres": m.get("genres", "N/A")}
            for m in movies_collection.find({"movieId": {"$in": movie_ids}})
        }

        # Créer un tableau d'évaluations avec genres
        ratings_display = [
            {
                "Titre": movies_data.get(r["movieId"], {}).get("title", "Inconnu"),
                "Genres": ", ".join(movies_data.get(r["movieId"], {}).get("genres", "").split("|")),
                "Note": r["rating"]
            }
            for r in user_ratings
        ]

        # Afficher les évaluations
        df = pd.DataFrame(ratings_display)
        st.subheader(f"🎬 Notes de l'utilisateur {selected_user} :")
        st.dataframe(df)

        # Graphique de répartition des notes
        df_ratings = pd.DataFrame(user_ratings)
        hist_chart = alt.Chart(df_ratings).mark_bar().encode(
            x=alt.X("rating:Q", bin=True, title="Note"),
            y=alt.Y("count():Q", title="Nombre de films"),
            tooltip=["count():Q"]
        ).properties(
            title="Répartition des notes de l'utilisateur",
            width=600
        )
        st.altair_chart(hist_chart)

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

            # Récupérer les données des films recommandés
            rec_movies_data = {
                m["movieId"]: {"title": m["title"], "genres": m.get("genres", "N/A")}
                for m in movie_lens_movies.find({"movieId": {"$in": rec_movie_ids}})
            }

            # Préparer les données à afficher
            recommendations_display = [
                {
                    "Titre recommandé": rec_movies_data.get(r["movieId"], {}).get("title", "Inconnu"),
                    "Genres": ", ".join(rec_movies_data.get(r["movieId"], {}).get("genres", "").split("|")),
                    "Score": round(r["rating"], 2),
                }
                for r in top_recs
            ]

            # Afficher les recommandations
            st.subheader("📈 Recommandations personnalisées")
            st.dataframe(pd.DataFrame(recommendations_display))
        else:
            st.info("Aucune recommandation disponible pour cet utilisateur.")
    else:
        st.warning("Aucun utilisateur avec cet ID trouvé dans la base de données.")

except Exception as e:
    st.error(f"Erreur de connexion à MongoDB : {e}")
