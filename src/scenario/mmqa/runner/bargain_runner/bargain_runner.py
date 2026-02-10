import pandas as pd
from typing import List

# Add parent directory to path for imports
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
# from src.runner.generic_bargain_runner.generic_bargain_runner import GenericBargainRunner

from runner.generic_bargain_runner.generic_bargain_runner import GenericBargainRunner


class BargainRunner(GenericBargainRunner):
    """Runner for the BARGAIN system in MMQA scenario."""

    def __init__(
        self,
        use_case: str,
        scale_factor: int,
        model_name: str = "gpt-4o-mini",
        concurrent_llm_worker=20,
        skip_setup: bool = False,
    ):
        """
        Initialize BARGAIN runner for MMQA.

        Args:
            use_case: The use case to run
            model_name: LLM model to use for proxy (cheap model)
        """
        super().__init__(
            use_case,
            scale_factor,
            model_name,
            concurrent_llm_worker,
            skip_setup,
        )

    def _execute_q1(self):
        """
        Execute Q1: "Extract the director name from the movie description."
        
        This query processes the ben_piazza dataset to extract director names
        from movie descriptions for rows where Role is "Bob Whitewood".
        """
        # Load and join data as in the Palimpzest implementation
        table_df = self.load_data("ben_piazza.csv")
        text_df = self.load_data("ben_piazza_text_data.csv")
        joined_df = table_df.merge(
            text_df,
            left_on="Title",
            right_on="title",
            how="left",
        )
        joined_df = joined_df.fillna("")
        
        # Filter for rows where Role is "Bob Whitewood"
        filtered_df = joined_df[joined_df["Role"] == "Bob Whitewood"]
        
        if filtered_df.empty:
            return pd.DataFrame(columns=["director"])
        
        # Extract text descriptions
        text_descriptions = filtered_df["text"].tolist()
        
        # Define task template for BARGAIN
        task_template = "Extract the director name from the following movie description: {}"
        
        # Process with BARGAIN
        results = self.process_with_bargain(text_descriptions, task_template, is_binary=False)
        
        # Create results DataFrame
        results_df = pd.DataFrame({
            "director": results
        })
        
        return results_df

    def _execute_q4(self):
        """
        Execute Q4: Extract genres from movie descriptions and create genre-movies mapping.
        
        This query processes the lizzy_caplan_text_data dataset to extract genres
        from movie descriptions and creates a mapping of genres to movies.
        """
        # Load data
        text_df = self.load_data(
            "lizzy_caplan_text_data.csv", sep=",", quotechar='"'
        )
        
        # Target movies as specified in the original query
        target_values = [
            "Orange County",
            "Mean Girls",
            "Love Is the Drug",
            "Crashing",
            "Cloverfield",
            "My Best Friend's Girl",
            "Crossing Over",
            "Hot Tub Time Machine",
            "The Last Rites of Ransom Pride",
            "127 Hours",
            "High Road",
            "Save the Date",
            "Bachelorette",
            "3, 2, 1... Frankie Go Boom",
            "Queens of Country",
            "Item 47",
            "The Interview",
            "The Night Before",
            "Now You See Me 2",
            "Allied",
            "The Disaster Artist",
            "Extinction",
            "The People We Hate at the Wedding",
            "Cobweb",
        ]
        
        # Filter for target movies
        text_df = text_df[text_df["title"].isin(target_values)]
        
        if text_df.empty:
            return pd.DataFrame(columns=["genre", "movies_in_genre"])
        
        # Extract text descriptions
        text_descriptions = text_df["text"].tolist()
        movie_titles = text_df["title"].tolist()
        
        # Define task template for BARGAIN
        task_template = "Extract the genres of the movie from the following description. Return genres separated by commas: {}"
        
        # Process with BARGAIN
        genre_results = self.process_with_bargain(text_descriptions, task_template, is_binary=False)
        
        # Process results to create genre-movies mapping
        expanded_data = []
        for movie_title, genre_string in zip(movie_titles, genre_results):
            if isinstance(genre_string, str):
                # Split genres by comma and clean up
                genres = [genre.lower().strip() for genre in genre_string.split(",")]
                for genre in genres:
                    if genre:  # Skip empty genres
                        expanded_data.append({"genre": genre, "title": movie_title})
        
        if not expanded_data:
            return pd.DataFrame(columns=["genre", "movies_in_genre"])
        
        # Create DataFrame and group by genre
        df_expanded = pd.DataFrame(expanded_data)
        genre_movies_table = (
            df_expanded.groupby("genre")["title"]
            .apply(lambda x: ", ".join(x))
            .reset_index()
        )
        genre_movies_table.rename(
            columns={"title": "movies_in_genre"}, inplace=True
        )
        
        return genre_movies_table

    def _execute_q5(self):
        """
        Execute Q5: "Who has played a role in all the movies listed in the table 
        given their descriptions? Simply give the name of the actor."
        
        This query processes the lizzy_caplan_text_data dataset to find an actor
        who has appeared in all the specified movies.
        """
        # Load data
        text_df = self.load_data(
            "lizzy_caplan_text_data.csv", sep=",", quotechar='"'
        )
        
        # Target movies as specified in the original query
        target_values = [
            "Love Is the Drug",
            "Crashing",
            "Cloverfield",
            "My Best Friend's Girl",
            "Hot Tub Time Machine",
            "The Last Rites of Ransom Pride",
            "Save the Date",
            "Bachelorette",
            "3, 2, 1... Frankie Go Boom",
            "Queens of Country",
            "Item 47",
            "The Night Before",
            "Now You See Me 2",
            "Allied",
            "Extinction",
            "Cobweb",
        ]
        
        # Filter for target movies
        text_df = text_df[text_df["title"].isin(target_values)]
        
        if text_df.empty:
            return pd.DataFrame(columns=["actor"])
        
        # Extract text descriptions
        text_descriptions = text_df["text"].tolist()
        
        # Define task template for BARGAIN
        # Note: This is a complex query that requires reasoning across multiple documents
        # We'll process each description to extract actors, then find common ones
        task_template = "Extract the main actor names from the following movie description. Return names separated by commas: {}"
        
        # Process with BARGAIN
        actor_results = self.process_with_bargain(text_descriptions, task_template, is_binary=False)
        
        # Process results to find common actor
        all_actors = []
        for actor_string in actor_results:
            if isinstance(actor_string, str):
                actors = [actor.strip() for actor in actor_string.split(",") if actor.strip()]
                all_actors.append(set(actors))
        
        # Find intersection of all actor sets
        if all_actors:
            common_actors = set.intersection(*all_actors)
        else:
            common_actors = set()
        
        # Return results
        if common_actors:
            # Join multiple actors if found
            actor_names = ", ".join(sorted(common_actors))
        else:
            actor_names = "No common actor found"
        
        return pd.DataFrame({"actor": [actor_names]})

    def _get_empty_results_dataframe(self, query_id: int) -> pd.DataFrame:
        """
        Get empty DataFrame with correct columns for a query.
        
        Args:
            query_id: ID of the query

        Returns:
            Empty DataFrame with correct columns
        """
        if query_id == 1:
            return pd.DataFrame(columns=["director"])
        elif query_id == 4:
            return pd.DataFrame(columns=["genre", "movies_in_genre"])
        elif query_id == 5:
            return pd.DataFrame(columns=["actor"])
        else:
            return pd.DataFrame()
