# database/queries.py

from typing import List, Dict
import logging

# Import ONLY get_cursor. get_pool is not needed here.
from database.connection import get_cursor

logger = logging.getLogger(__name__)

def get_tournament_teams(tournament_id: int) -> List[Dict[str, str]]:
    """
    Retrieves a list of teams for a given tournament from the database.
    """
    # SQL query with a %s placeholder for secure parameter passing
    sql_query = """
        SELECT
            a.src_id as area_src_id,
            t.src_id as team_src_id,
            t.name as team_name
        FROM
            teams t
        JOIN
            tournament_teams tt ON t.id = tt.team_id
        JOIN
            tournaments trn ON tt.tournament_id = trn.id
        JOIN
            areas a ON trn.area_id = a.id
        WHERE
            trn.id = %s
        ORDER BY
            t.name
        LIMIT 4;
    """

    try:
        # Simply use get_cursor. It will take a connection from the pool
        # which is already open in the main() function.
        with get_cursor() as cur:
            # Execute the query, passing parameters as a tuple in the second argument
            cur.execute(sql_query, (tournament_id,))
            
            # fetchall() will return a list of dictionaries, as row_factory=dict_row is set in the pool
            db_results = cur.fetchall()

        # Form the final list, ensuring all values are strings
        teams_list = [
            {
                "area_src_id": str(row["area_src_id"]),
                "team_src_id": str(row["team_src_id"]),
                "team_name": str(row["team_name"])
            }
            for row in db_results
        ]
        
        logger.info(f"Found {len(teams_list)} teams for tournament_id={tournament_id}")
        return teams_list

    except Exception as e:
        logger.error(f"Error fetching teams for tournament_id={tournament_id}: {e}", exc_info=True)
        # In case of a database error, it's safer to return an empty list
        # to avoid crashing the calling code.
        return []