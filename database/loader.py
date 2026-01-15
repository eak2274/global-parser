# database\loader.py

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Tuple

# Add the project root directory to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Import only the necessary utility
from database.connection import check_connection, close_pool

# --- Logging Setup ---
def setup_logging():
    """Sets up logging to a file in the 'logs' directory."""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"loader_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def convert_unix_timestamp(unix_ts: int) -> datetime:
    """Converts a Unix timestamp to a datetime object with UTC timezone."""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc)

def validate_json_file(file_path: str) -> bool:
    """Checks for file existence and validates JSON structure."""
    if not os.path.exists(file_path):
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            if not isinstance(data, list):
                return False
            
            required_fields = ['area_src_id', 'area_name', 'tourney_src_id', 'tourney_name', 'results']
            for item in data:
                for field in required_fields:
                    if field not in item:
                        return False
                
                result_required_fields = ['game_src_id', 'game_ts', 'game_end', 'home_src_id', 'away_src_id']
                for result in item['results']:
                    for field in result_required_fields:
                        if field not in result:
                            return False
            
            return True
    except json.JSONDecodeError:
        return False

# =============================================================================
# LEVEL 3: Process single game
# =============================================================================

def process_game_with_update(
    cur, 
    game_data: dict, 
    tourney_id: int, 
    teams_cache: dict, 
    current_tournament_team_links: set
) -> dict:
    """
    Processes a single game with UPDATE for existing records.
    
    Args:
        cur: Active database cursor
        game_data: Single game data from results array
        tourney_id: Tournament ID in database
        teams_cache: Cache dict {team_src_id: team_db_id}
        current_tournament_team_links: Set of (tournament_id, team_id) tuples
    
    Returns:
        dict: Statistics of operations performed
    """
    stats = {
        'teams': {'created': 0, 'updated': 0},
        'tournament_teams_links': {'created': 0},
        'games': {'created': 0, 'updated': 0}
    }
    
    # Process Home Team
    home_src_id = game_data['home_src_id']
    if home_src_id not in teams_cache:
        cur.execute("SELECT id FROM teams WHERE src_id = %s", (home_src_id,))
        result = cur.fetchone()
        
        if result:
            home_team_id = result['id']
            cur.execute(
                """UPDATE teams SET name = %s, slug = %s, abbr = %s, logo = %s, updated_at = NOW() WHERE id = %s""",
                (game_data.get('home_name'), game_data.get('home_slug'), 
                 game_data.get('home_abbr'), game_data.get('home_logo'), home_team_id)
            )
            stats['teams']['updated'] += 1
        else:
            cur.execute(
                """INSERT INTO teams (src_id, name, slug, abbr, logo) 
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (home_src_id, game_data.get('home_name'), game_data.get('home_slug'), 
                 game_data.get('home_abbr'), game_data.get('home_logo'))
            )
            home_team_id = cur.fetchone()['id']
            stats['teams']['created'] += 1
        
        teams_cache[home_src_id] = home_team_id
    else:
        home_team_id = teams_cache[home_src_id]

    # Create tournament-team link for home team
    if (tourney_id, home_team_id) not in current_tournament_team_links:
        cur.execute(
            "INSERT INTO tournament_teams (tournament_id, team_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (tourney_id, home_team_id)
        )
        if cur.rowcount > 0:
            stats['tournament_teams_links']['created'] += 1
        current_tournament_team_links.add((tourney_id, home_team_id))

    # Process Away Team
    away_src_id = game_data['away_src_id']
    if away_src_id not in teams_cache:
        cur.execute("SELECT id FROM teams WHERE src_id = %s", (away_src_id,))
        result = cur.fetchone()
        
        if result:
            away_team_id = result['id']
            cur.execute(
                """UPDATE teams SET name = %s, slug = %s, abbr = %s, logo = %s, updated_at = NOW() WHERE id = %s""",
                (game_data.get('away_name'), game_data.get('away_slug'), 
                 game_data.get('away_abbr'), game_data.get('away_logo'), away_team_id)
            )
            stats['teams']['updated'] += 1
        else:
            cur.execute(
                """INSERT INTO teams (src_id, name, slug, abbr, logo) 
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (away_src_id, game_data.get('away_name'), game_data.get('away_slug'), 
                 game_data.get('away_abbr'), game_data.get('away_logo'))
            )
            away_team_id = cur.fetchone()['id']
            stats['teams']['created'] += 1
        
        teams_cache[away_src_id] = away_team_id
    else:
        away_team_id = teams_cache[away_src_id]

    # Create tournament-team link for away team
    if (tourney_id, away_team_id) not in current_tournament_team_links:
        cur.execute(
            "INSERT INTO tournament_teams (tournament_id, team_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (tourney_id, away_team_id)
        )
        if cur.rowcount > 0:
            stats['tournament_teams_links']['created'] += 1
        current_tournament_team_links.add((tourney_id, away_team_id))
    
    # Process Game
    game_src_id = game_data['game_src_id']
    cur.execute("SELECT id FROM games WHERE src_id = %s", (game_src_id,))
    result = cur.fetchone()
    
    if result:
        game_id = result['id']
        cur.execute(
            """UPDATE games SET 
               tournament_id = %s, home_team_id = %s, away_team_id = %s,
               game_ts = %s, game_end = %s, home_score = %s, away_score = %s,
               home_q1 = %s, home_q2 = %s, home_q3 = %s, home_q4 = %s,
               home_ot1 = %s, home_ot2 = %s, away_q1 = %s, away_q2 = %s,
               away_q3 = %s, away_q4 = %s, away_ot1 = %s, away_ot2 = %s,
               updated_at = NOW() 
               WHERE id = %s""",
            (tourney_id, home_team_id, away_team_id,
             convert_unix_timestamp(game_data['game_ts']), game_data['game_end'], 
             game_data.get('home_score'), game_data.get('away_score'),
             game_data.get('home_q1'), game_data.get('home_q2'), 
             game_data.get('home_q3'), game_data.get('home_q4'), 
             game_data.get('home_ot1'), game_data.get('home_ot2'),
             game_data.get('away_q1'), game_data.get('away_q2'), 
             game_data.get('away_q3'), game_data.get('away_q4'), 
             game_data.get('away_ot1'), game_data.get('away_ot2'),
             game_id)
        )
        stats['games']['updated'] += 1
    else:
        cur.execute(
            """INSERT INTO games 
               (src_id, tournament_id, home_team_id, away_team_id,
               game_ts, game_end, home_score, away_score,
               home_q1, home_q2, home_q3, home_q4,
               home_ot1, home_ot2, away_q1, away_q2,
               away_q3, away_q4, away_ot1, away_ot2) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (game_src_id, tourney_id, home_team_id, away_team_id,
             convert_unix_timestamp(game_data['game_ts']), game_data['game_end'], 
             game_data.get('home_score'), game_data.get('away_score'),
             game_data.get('home_q1'), game_data.get('home_q2'), 
             game_data.get('home_q3'), game_data.get('home_q4'), 
             game_data.get('home_ot1'), game_data.get('home_ot2'),
             game_data.get('away_q1'), game_data.get('away_q2'), 
             game_data.get('away_q3'), game_data.get('away_q4'), 
             game_data.get('away_ot1'), game_data.get('away_ot2'))
        )
        game_id = cur.fetchone()['id']
        stats['games']['created'] += 1

    return stats


def process_game_insert_only(
    cur, 
    game_data: dict, 
    tourney_id: int, 
    teams_cache: dict, 
    current_tournament_team_links: set
) -> dict:
    """
    Processes a single game with INSERT ONLY (skips existing records).
    
    Args:
        cur: Active database cursor
        game_data: Single game data from results array
        tourney_id: Tournament ID in database
        teams_cache: Cache dict {team_src_id: team_db_id}
        current_tournament_team_links: Set of (tournament_id, team_id) tuples
    
    Returns:
        dict: Statistics of operations performed
    """
    stats = {
        'teams': {'created': 0, 'skipped': 0},
        'tournament_teams_links': {'created': 0},
        'games': {'created': 0, 'skipped': 0}
    }
    
    # Process Home Team (INSERT only)
    home_src_id = game_data['home_src_id']
    if home_src_id not in teams_cache:
        cur.execute("SELECT id FROM teams WHERE src_id = %s", (home_src_id,))
        result = cur.fetchone()
        
        if result:
            home_team_id = result['id']
            stats['teams']['skipped'] += 1
        else:
            cur.execute(
                """INSERT INTO teams (src_id, name, slug, abbr, logo) 
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (home_src_id, game_data.get('home_name'), game_data.get('home_slug'), 
                 game_data.get('home_abbr'), game_data.get('home_logo'))
            )
            home_team_id = cur.fetchone()['id']
            stats['teams']['created'] += 1
        
        teams_cache[home_src_id] = home_team_id
    else:
        home_team_id = teams_cache[home_src_id]

    # Create tournament-team link for home team
    if (tourney_id, home_team_id) not in current_tournament_team_links:
        cur.execute(
            "INSERT INTO tournament_teams (tournament_id, team_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (tourney_id, home_team_id)
        )
        if cur.rowcount > 0:
            stats['tournament_teams_links']['created'] += 1
        current_tournament_team_links.add((tourney_id, home_team_id))

    # Process Away Team (INSERT only)
    away_src_id = game_data['away_src_id']
    if away_src_id not in teams_cache:
        cur.execute("SELECT id FROM teams WHERE src_id = %s", (away_src_id,))
        result = cur.fetchone()
        
        if result:
            away_team_id = result['id']
            stats['teams']['skipped'] += 1
        else:
            cur.execute(
                """INSERT INTO teams (src_id, name, slug, abbr, logo) 
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (away_src_id, game_data.get('away_name'), game_data.get('away_slug'), 
                 game_data.get('away_abbr'), game_data.get('away_logo'))
            )
            away_team_id = cur.fetchone()['id']
            stats['teams']['created'] += 1
        
        teams_cache[away_src_id] = away_team_id
    else:
        away_team_id = teams_cache[away_src_id]

    # Create tournament-team link for away team
    if (tourney_id, away_team_id) not in current_tournament_team_links:
        cur.execute(
            "INSERT INTO tournament_teams (tournament_id, team_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (tourney_id, away_team_id)
        )
        if cur.rowcount > 0:
            stats['tournament_teams_links']['created'] += 1
        current_tournament_team_links.add((tourney_id, away_team_id))
    
    # Process Game (INSERT only)
    game_src_id = game_data['game_src_id']
    cur.execute("SELECT id FROM games WHERE src_id = %s", (game_src_id,))
    result = cur.fetchone()
    
    if result:
        stats['games']['skipped'] += 1
    else:
        cur.execute(
            """INSERT INTO games 
               (src_id, tournament_id, home_team_id, away_team_id,
               game_ts, game_end, home_score, away_score,
               home_q1, home_q2, home_q3, home_q4,
               home_ot1, home_ot2, away_q1, away_q2,
               away_q3, away_q4, away_ot1, away_ot2) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (game_src_id, tourney_id, home_team_id, away_team_id,
             convert_unix_timestamp(game_data['game_ts']), game_data['game_end'], 
             game_data.get('home_score'), game_data.get('away_score'),
             game_data.get('home_q1'), game_data.get('home_q2'), 
             game_data.get('home_q3'), game_data.get('home_q4'), 
             game_data.get('home_ot1'), game_data.get('home_ot2'),
             game_data.get('away_q1'), game_data.get('away_q2'), 
             game_data.get('away_q3'), game_data.get('away_q4'), 
             game_data.get('away_ot1'), game_data.get('away_ot2'))
        )
        game_id = cur.fetchone()['id']
        stats['games']['created'] += 1

    return stats


# =============================================================================
# LEVEL 2: Process single tournament with all its games
# =============================================================================

def process_tournament_with_update(
    cur,
    tournament_data: dict,
    areas_cache: dict,
    teams_cache: dict
) -> dict:
    """
    Processes a single tournament with all its games (with UPDATE).
    
    Args:
        cur: Active database cursor
        tournament_data: Single tournament data with area, tournament info and results
        areas_cache: Cache dict {area_src_id: area_db_id}
        teams_cache: Cache dict {team_src_id: team_db_id}
    
    Returns:
        dict: Aggregated statistics of operations performed
    """
    stats = {
        'areas': {'created': 0, 'updated': 0},
        'tournaments': {'created': 0, 'updated': 0},
        'teams': {'created': 0, 'updated': 0},
        'tournament_teams_links': {'created': 0},
        'games': {'created': 0, 'updated': 0}
    }
    
    # Process Area
    area_src_id = tournament_data['area_src_id']
    area_name = tournament_data['area_name']
    
    if area_src_id not in areas_cache:
        cur.execute("SELECT id FROM areas WHERE src_id = %s", (area_src_id,))
        result = cur.fetchone()
        
        if result:
            area_id = result['id']
            cur.execute("UPDATE areas SET name = %s, updated_at = NOW() WHERE id = %s", (area_name, area_id))
            stats['areas']['updated'] += 1
        else:
            cur.execute("INSERT INTO areas (src_id, name) VALUES (%s, %s) RETURNING id", (area_src_id, area_name))
            area_id = cur.fetchone()['id']
            stats['areas']['created'] += 1
        
        areas_cache[area_src_id] = area_id
    else:
        area_id = areas_cache[area_src_id]

    # Process Tournament
    tourney_src_id = tournament_data['tourney_src_id']
    tourney_name = tournament_data['tourney_name']
    
    cur.execute("SELECT id FROM tournaments WHERE src_id = %s", (tourney_src_id,))
    result = cur.fetchone()
    
    if result:
        tourney_id = result['id']
        cur.execute(
            """UPDATE tournaments SET 
               area_id = %s, name = %s, code = %s, url = %s, 
               logo = %s, status = %s, updated_at = NOW() 
               WHERE id = %s""",
            (area_id, tourney_name, tournament_data.get('tourney_code'), 
             tournament_data.get('tourney_url'), tournament_data.get('tourney_logo'), 
             tournament_data.get('tourney_status'), tourney_id)
        )
        stats['tournaments']['updated'] += 1
    else:
        cur.execute(
            """INSERT INTO tournaments 
               (src_id, area_id, name, code, url, logo, status) 
               VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (tourney_src_id, area_id, tourney_name, tournament_data.get('tourney_code'), 
             tournament_data.get('tourney_url'), tournament_data.get('tourney_logo'), 
             tournament_data.get('tourney_status'))
        )
        tourney_id = cur.fetchone()['id']
        stats['tournaments']['created'] += 1

    # Process all games in this tournament
    current_tournament_team_links = set()
    
    for game_data in tournament_data['results']:
        game_stats = process_game_with_update(
            cur=cur,
            game_data=game_data,
            tourney_id=tourney_id,
            teams_cache=teams_cache,
            current_tournament_team_links=current_tournament_team_links
        )
        
        # Aggregate statistics
        stats['teams']['created'] += game_stats['teams']['created']
        stats['teams']['updated'] += game_stats['teams']['updated']
        stats['tournament_teams_links']['created'] += game_stats['tournament_teams_links']['created']
        stats['games']['created'] += game_stats['games']['created']
        stats['games']['updated'] += game_stats['games']['updated']
    
    return stats


def process_tournament_insert_only(
    cur,
    tournament_data: dict,
    areas_cache: dict,
    teams_cache: dict
) -> dict:
    """
    Processes a single tournament with all its games (INSERT ONLY).
    
    Args:
        cur: Active database cursor
        tournament_data: Single tournament data with area, tournament info and results
        areas_cache: Cache dict {area_src_id: area_db_id}
        teams_cache: Cache dict {team_src_id: team_db_id}
    
    Returns:
        dict: Aggregated statistics of operations performed
    """
    stats = {
        'areas': {'created': 0, 'skipped': 0},
        'tournaments': {'created': 0, 'skipped': 0},
        'teams': {'created': 0, 'skipped': 0},
        'tournament_teams_links': {'created': 0},
        'games': {'created': 0, 'skipped': 0}
    }
    
    # Process Area (INSERT only)
    area_src_id = tournament_data['area_src_id']
    area_name = tournament_data['area_name']
    
    if area_src_id not in areas_cache:
        cur.execute("SELECT id FROM areas WHERE src_id = %s", (area_src_id,))
        result = cur.fetchone()
        
        if result:
            area_id = result['id']
            stats['areas']['skipped'] += 1
        else:
            cur.execute("INSERT INTO areas (src_id, name) VALUES (%s, %s) RETURNING id", (area_src_id, area_name))
            area_id = cur.fetchone()['id']
            stats['areas']['created'] += 1
        
        areas_cache[area_src_id] = area_id
    else:
        area_id = areas_cache[area_src_id]

    # Process Tournament (INSERT only)
    tourney_src_id = tournament_data['tourney_src_id']
    tourney_name = tournament_data['tourney_name']
    
    cur.execute("SELECT id FROM tournaments WHERE src_id = %s", (tourney_src_id,))
    result = cur.fetchone()
    
    if result:
        tourney_id = result['id']
        stats['tournaments']['skipped'] += 1
    else:
        cur.execute(
            """INSERT INTO tournaments 
               (src_id, area_id, name, code, url, logo, status) 
               VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (tourney_src_id, area_id, tourney_name, tournament_data.get('tourney_code'), 
             tournament_data.get('tourney_url'), tournament_data.get('tourney_logo'), 
             tournament_data.get('tourney_status'))
        )
        tourney_id = cur.fetchone()['id']
        stats['tournaments']['created'] += 1

    # Process all games in this tournament
    current_tournament_team_links = set()
    
    for game_data in tournament_data['results']:
        game_stats = process_game_insert_only(
            cur=cur,
            game_data=game_data,
            tourney_id=tourney_id,
            teams_cache=teams_cache,
            current_tournament_team_links=current_tournament_team_links
        )
        
        # Aggregate statistics
        stats['teams']['created'] += game_stats['teams']['created']
        stats['teams']['skipped'] += game_stats['teams']['skipped']
        stats['tournament_teams_links']['created'] += game_stats['tournament_teams_links']['created']
        stats['games']['created'] += game_stats['games']['created']
        stats['games']['skipped'] += game_stats['games']['skipped']
    
    return stats


# =============================================================================
# LEVEL 1: Process entire JSON file
# =============================================================================

def load_to_db(cur, file_in: str):
    """
    Loads data from a JSON file into the database with UPDATE for existing records.
    
    This function does not manage the connection or transaction lifecycle.
    It assumes the caller handles connection creation and transaction management.
    
    Args:
        cur: An active database cursor object.
        file_in (str): Path to the input JSON file.
    """
    logger = logging.getLogger(__name__)
    
    source_file_path = Path(file_in)
    
    if not validate_json_file(file_in):
        logger.error(f"Invalid or missing JSON file: {file_in}")
        raise ValueError(f"Invalid or missing JSON file: {file_in}")
    
    logger.info(f"Processing file: {source_file_path}")
    
    stats = {
        'areas': {'created': 0, 'updated': 0},
        'tournaments': {'created': 0, 'updated': 0},
        'teams': {'created': 0, 'updated': 0},
        'tournament_teams_links': {'created': 0},
        'games': {'created': 0, 'updated': 0}
    }
    
    areas_cache = {}
    teams_cache = {}

    with open(source_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for tournament_data in data:
        tournament_stats = process_tournament_with_update(
            cur=cur,
            tournament_data=tournament_data,
            areas_cache=areas_cache,
            teams_cache=teams_cache
        )
        
        # Aggregate statistics
        for entity_type in stats:
            for operation in stats[entity_type]:
                stats[entity_type][operation] += tournament_stats[entity_type][operation]

    logger.info(f"Finished processing file: {source_file_path}. Stats:")
    for entity_type, counts in stats.items():
        created = counts.get('created', 0)
        updated = counts.get('updated', 0)
        if created > 0 or updated > 0:
            logger.info(f"  - {entity_type.capitalize()}: Created {created}, Updated {updated}")
    
    return stats


def load_to_db_insert_only(cur, file_in: str):
    """
    Loads data from a JSON file into the database with INSERT ONLY (skips existing records).
    
    This function does not manage the connection or transaction lifecycle.
    It assumes the caller handles connection creation and transaction management.
    
    Args:
        cur: An active database cursor object.
        file_in (str): Path to the input JSON file.
    """
    logger = logging.getLogger(__name__)
    
    source_file_path = Path(file_in)
    
    if not validate_json_file(file_in):
        logger.error(f"Invalid or missing JSON file: {file_in}")
        raise ValueError(f"Invalid or missing JSON file: {file_in}")
    
    logger.info(f"Processing file (INSERT ONLY): {source_file_path}")
    
    stats = {
        'areas': {'created': 0, 'skipped': 0},
        'tournaments': {'created': 0, 'skipped': 0},
        'teams': {'created': 0, 'skipped': 0},
        'tournament_teams_links': {'created': 0},
        'games': {'created': 0, 'skipped': 0}
    }
    
    areas_cache = {}
    teams_cache = {}

    with open(source_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for tournament_data in data:
        tournament_stats = process_tournament_insert_only(
            cur=cur,
            tournament_data=tournament_data,
            areas_cache=areas_cache,
            teams_cache=teams_cache
        )
        
        # Aggregate statistics
        for entity_type in stats:
            for operation in stats[entity_type]:
                stats[entity_type][operation] += tournament_stats[entity_type][operation]

    logger.info(f"Finished processing file: {source_file_path}. Stats:")
    for entity_type, counts in stats.items():
        created = counts.get('created', 0)
        skipped = counts.get('skipped', 0)
        if created > 0 or skipped > 0:
            logger.info(f"  - {entity_type.capitalize()}: Created {created}, Skipped {skipped}")
    
    return stats


# =============================================================================
# STANDALONE SCRIPT EXECUTION (for manual testing)
# =============================================================================

if __name__ == "__main__":
    logger = setup_logging()
    
    if len(sys.argv) < 2:
        print("Usage: python loader.py <path_to_json_file> [--insert-only]")
        sys.exit(1)

    file_path = sys.argv[1]
    insert_only = '--insert-only' in sys.argv
    
    try:
        if not check_connection():
            logger.error("Could not connect to the database")
            sys.exit(1)
            
        from database.connection import transaction
        with transaction() as cur:
            if insert_only:
                logger.info("Running in INSERT ONLY mode")
                load_to_db(cur, file_path)
                
    except Exception as e:
        logger.error(f"Critical error while processing file {file_path}: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Close the pool when the standalone script finishes
        logger.info("Closing connection pool...")
        close_pool()