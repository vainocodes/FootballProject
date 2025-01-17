import pandas as pd #funktioita
import json
import sqlite3
# Import necessary libraries


# Load JSON file
with open('15946.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Create player_positions, teams, positions, players,
# countries and cards dataframe
positions = pd.json_normalize(
    data,
    record_path=['lineup', 'positions'],
    meta=[],
    errors='ignore'
)
# Drop columns not needed for the player_positions dataframe
positions.drop(columns=["from", "to", "from_period", "to_period", "start_reason", "end_reason"], inplace=True)

# Create function to group positions
def group_position(pos):
    if pos == "Goalkeeper":
        return "Goalkeeper"
    elif pos in ["Left Center Back", "Right Center Back", "Centre Back"]:
        return "Centre Back"
    elif pos in ["Left Back"]:
        return "Left Back"
    elif pos in ["Right Back"]:
        return "Right Back"
    elif pos in ["Left Center Midfield", "Right Center Midfield", "Left Defensive Midfield",
                 "Right Defensive Midfield"]:
        return "Midfield"
    elif pos in ["Left Wing", "Right Wing", "Left Midfield", "Right Midfield"]:
        return "Winger"
    elif pos in ["Center Forward", "Left Center Forward", "Right Center Forward", "Attacker", "Striker"]:
        return "Attacker"
    else:
        return "Other"

# Lisää position_group-sarake DataFrameen
positions['position_group'] = positions['position'].apply(group_position)

positions = positions.drop_duplicates(subset=['position'])


teams = pd.json_normalize(data, meta=['team_id', 'team_name'])
# Choose only necessary columns
teams = teams[['team_id', 'team_name']]
# print(teams.head())
# print(teams.dtypes)

player_positions = pd.json_normalize(
    data,
    record_path=['lineup', 'positions'],
    meta=[['lineup', 'player_id'], ['lineup', 'player_name']],
    errors='ignore'
)
player_positions.rename(columns={'lineup.player_id': 'player_id'}, inplace=True)
player_positions.rename(columns={'lineup.player_name': 'player_name'}, inplace=True)
player_positions.drop(columns=["position"], inplace=True)
# Rename 'from' and 'to'
player_positions.rename(columns={'from': 'from_time', 'to': 'to_time'}, inplace=True)
# If to_time value = none, make it 90.00
player_positions['to_time'] = player_positions['to_time'].fillna('90:00')


# Function to adjust minutes and seconds for visualization
def parse_time_to_minutes(value):
    if pd.isna(value):
        return 0
    try:
        minutes, seconds = map(int, value.split(':'))
        return minutes + seconds / 60
    except:
        return 0

# Change time to minutes
player_positions['from_time_minutes'] = player_positions['from_time'].apply(parse_time_to_minutes)
player_positions['to_time_minutes'] = player_positions['to_time'].apply(parse_time_to_minutes)

# Count minutes
player_positions['minutes_played'] = player_positions['to_time_minutes'] - player_positions['from_time_minutes']

players = pd.json_normalize(
    data,
    record_path='lineup',
    meta=['team_id'],
    errors='ignore'
)
players.rename(columns={"country.id": "country_id"}, inplace=True)
players.drop(columns=["country.name"], inplace=True)
players.drop(columns=["cards"], inplace=True)
players.drop(columns=["positions"], inplace=True)
# Show all columns when printing
pd.set_option('display.max_columns', None)

countries = pd.json_normalize(
    data,
    record_path='lineup',
    meta=[],
    errors='ignore'
)[['country.id', 'country.name']].drop_duplicates()

countries.rename(columns={'country.id': 'country_id', 'country.name': 'country_name'}, inplace=True)

cards = pd.json_normalize(
    data,
    record_path=['lineup', 'cards'],  # Sukella cards-listaan
    meta=[['lineup', 'player_id']],
    errors='ignore'
)
cards.rename(columns={'lineup.player_id': 'player_id'}, inplace=True)
# print(cards)

# Add 'card_id' which has a unique growing value
cards['card_id'] = range(1, len(cards) + 1)



# Move player_id left
columns_order = ['player_id', 'card_id'] + [col for col in cards.columns if col not in ['player_id', 'card_id']]
cards = cards[columns_order]


# ------SQL-------

# Connect to the database
conn = sqlite3.connect('football.db')

# Create cursor for SQL
cursor = conn.cursor()

# Create all the necessary tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Positions (
    position_id INTEGER,
    position TEXT,
    position_group TEXT,
    PRIMARY KEY (position_id)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Teams (
    team_id INTEGER PRIMARY KEY,
    team_name TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS PlayerPositions (
    position_id INTEGER,
    from_time TEXT,
    to_time TEXT,
    from_period INTEGER,
    to_period INTEGER,
    start_reason TEXT,
    end_reason TEXT,
    player_id INTEGER,
    from_time_minutes FLOAT,
    to_time_minutes FLOAT,
    minutes_played FLOAT,
    player_name TEXT,
    PRIMARY KEY (position_id, player_id),
    FOREIGN KEY (position_id) REFERENCES Positions(position_id),
    FOREIGN KEY (player_id) REFERENCES Players(player_id),
    FOREIGN KEY (player_name) REFERENCES Players(player_name)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Players (
    player_id INTEGER PRIMARY KEY,
    player_name TEXT,
    player_nickname TEXT,
    jersey_number INTEGER,
    country_id INTEGER,
    team_id INTEGER,
    FOREIGN KEY (country_id) REFERENCES Countries(country_id),
    FOREIGN KEY (team_id) REFERENCES Teams(team_id)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Cards (
    card_id INTEGER PRIMARY KEY,
    player_id INTEGER,
    time TEXT,
    card_type TEXT,
    reason TEXT,
    period INTEGER,
    FOREIGN KEY (player_id) REFERENCES Players(player_id)
)
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Countries (
    country_id INTEGER PRIMARY KEY,
    country_name TEXT
)
''')



# stop the cursor
conn.commit()

# Move the dataframes to the database
player_positions.to_sql('PlayerPositions', conn, if_exists='replace', index=False)
teams.to_sql('Teams', conn, if_exists='replace', index=False)

players.to_sql('Players', conn, if_exists='replace', index=False)

# positions = df[['position_id', 'position', 'player_id', 'team_id', 'from_time', 'to_time', 'from_period', 'to_period', 'start_reason', 'end_reason']].drop_duplicates()
positions.to_sql('Positions', conn, if_exists='replace', index=False)

cards.to_sql('Cards', conn, if_exists='replace', index=False)

countries.to_sql('Countries', conn, if_exists='replace', index=False)



# Check the tables have the wanted data
results_player_positions = pd.read_sql('SELECT * FROM PlayerPositions', conn)
print(results_player_positions)


results_teams = pd.read_sql('SELECT * FROM Teams', conn)
# print(results_teams)

results_positions = pd.read_sql('SELECT * FROM Positions', conn)
pd.set_option('display.max_columns', None)
print(results_positions)

results_players = pd.read_sql('SELECT * FROM Players', conn)
pd.set_option('display.max_columns', None)
# print(results_players)

results_cards = pd.read_sql('SELECT * FROM Cards', conn)
pd.set_option('display.max_columns', None)
# print(results_cards)

results_countries = pd.read_sql('SELECT * FROM Countries', conn)
pd.set_option('display.max_columns', None)
# print(results_countries)





