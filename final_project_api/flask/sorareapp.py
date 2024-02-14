from flask import Flask
from flask import abort
from flask_basicauth import BasicAuth
import pymysql
import os
import json
import math
from flask import request
from collections import defaultdict
from flask_swagger_ui import get_swaggerui_blueprint

app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi.yaml',
)
app.register_blueprint(swaggerui_blueprint)

MAX_PAGE_SIZE = 100

@app.route("/")
@auth.required
def hello_world():
    return "Hello, let's play with sorare stats!"

@app.route("/players/<string:player_slug>")
@auth.required
def player(player_slug):
    db_conn = pymysql.connect(
    host="localhost",
    user="root", 
    password=os.getenv('MySQLpwd'), 
    database="final_project",
    cursorclass=pymysql.cursors.DictCursor  
    )
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                player_name,
                age,
                u23_eligible
            FROM players_scores
            WHERE player_slug=%s;
            """, (player_slug,))
        player = cursor.fetchone()
        if not player:
            abort(404)
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                gameweek_date,
                gameweek_number,
                L5,
                L15,
                regular_status,
                Start15
            FROM players_scores
            WHERE player_slug = %s
            ORDER BY gameweek_date;
        """, (player_slug,))
        so5_stats = cursor.fetchall()
        player['so5_stats'] = so5_stats
    db_conn.close()
    return player


@app.route("/players",methods=["GET"])
@auth.required
def players():
    # URL parameters
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details', 0)))

    db_conn = pymysql.connect(
        host="localhost",
        user="root",
        password=os.getenv('MySQLpwd'),
        database="final_project",
        cursorclass=pymysql.cursors.DictCursor)
    # Get the players
    with db_conn.cursor() as cursor:
        cursor.execute("""
        SELECT
            player_slug,
            player_name,
            age,
            u23_eligible
        FROM players_scores
        ORDER BY player_slug
        LIMIT %s
        OFFSET %s
        """, (page_size, page * page_size))
        players = cursor.fetchall()
        player_slugs = [player['player_slug'] for player in players]
        placeholders = ', '.join(['%s'] * len(player_slugs))

    if include_details:
        # Get so5_stats
        with db_conn.cursor() as cursor:
            placeholder = ','.join(['%s'] * len(player_slugs))
            cursor.execute(f"""
                SELECT 
                    gameweek_date,
                    gameweek_number,
                    L5,
                    L15,
                    regular_status,
                    Start15,
                    player_slug  # Add player_slug column to the SELECT statement
                FROM players_scores
                WHERE player_slug in ({placeholder})
                ORDER BY gameweek_date;
            """, player_slugs)
            stats = cursor.fetchall()
            stats_dict = defaultdict(list)
            for obj in stats:
                player_slug = obj['player_slug']
                del obj['player_slug']
                stats_dict[player_slug].append(obj)

        # Merge so5_stats into players
        for player in players:
            player_slug = player['player_slug']
            player['stats'] = stats_dict[player_slug]

    # Get the total players count
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM players")
        total = cursor.fetchone()
        last_page = math.ceil(total['total'] / page_size)

    db_conn.close()
    return {
        'players': players,
        'next_page': f'/players?page={page + 1}&page_size={page_size}&include_details={int(include_details)}',
        'last_page': f'/players?page={last_page}&page_size={page_size}&include_details={int(include_details)}',
    }



