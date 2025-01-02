import pandas as pd #funktioita
import json
import sqlite3


# Lataa JSON-tiedosto
with open('15946.json', 'r') as file:
    data = json.load(file)

player_positions = pd.json_normalize(
    data,
    record_path=['lineup', 'positions'],
    meta=[['lineup', 'player_id']],
    errors='ignore'
)
player_positions.drop(columns=["position", "from", "to", "from_period", "to_period", "start_reason", "end_reason"], inplace=True)
player_positions.rename(columns={'lineup.player_id': 'player_id'}, inplace=True)
# print(player_positions)

# Normalisoi joukkue-, pelaaja, sekä pelipaikkadata
teams = pd.json_normalize(data, meta=['team_id', 'team_name'])
teams = teams[['team_id', 'team_name']]  # Valitse vain tarvittavat sarakkeet
# print(teams.head())
# print(teams.dtypes)




positions = pd.json_normalize(
    data,
    record_path=['lineup', 'positions'],
    meta=[],
    errors='ignore'
)
positions.rename(columns={'from': 'from_time', 'to': 'to_time'}, inplace=True)


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
pd.set_option('display.max_columns', None)


# Normalisoi 'lineup' ja poimi 'country.id' ja 'country.name'
countries = pd.json_normalize(
    data,
    record_path='lineup',
    meta=[],
    errors='ignore'
)[['country.id', 'country.name']].drop_duplicates()

# Nimeä sarakkeet selkeästi
countries.rename(columns={'country.id': 'country_id', 'country.name': 'country_name'}, inplace=True)

# print(countries.columns)
# Tarkista tulos
# print(countries)


# positions.rename(columns={'from': 'from_time', 'to': 'to_time'}, inplace=True)

cards = pd.json_normalize(
    data,
    record_path=['lineup', 'cards'],  # Sukella cards-listaan
    meta=[['lineup', 'player_id']],
    errors='ignore'
)
cards.rename(columns={'lineup.player_id': 'player_id'}, inplace=True)
# print(cards)

# Lisää card_id -sarake, jossa on uniikki arvo (esim. automaattisesti kasvava numero)
cards['card_id'] = range(1, len(cards) + 1)

# Tarkista tulos
# print(cards.columns)

# Siirrä player_id vasemmalle sarakkeiden järjestyksessä
columns_order = ['player_id', 'card_id'] + [col for col in cards.columns if col not in ['player_id', 'card_id']]
cards = cards[columns_order]

# Tarkista tulos
# print(cards)


# positions = pd.merge(positions, teams, on='team_id', how='left')

# Täydennä mahdollisesti puuttuvat tiedot
# positions['team_name'] = positions['team_name'].fillna('Unknown Team')  # Esimerkki oletusarvosta
# positions['player_id'] = positions['lineup.player_id']  # Varmista, että player_id on mukana


# pd.set_option('display.max_columns', None)
# print(positions)

# ------SQL-------

# Luodaan yhteys tietokantaan
conn = sqlite3.connect('football.db')

# Luo kursori SQL-komentojen suorittamiseen
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS PlayerPositions (
    position_id INTEGER,
    player_id INTEGER,
    PRIMARY KEY (position_id, player_id),
    FOREIGN KEY (position_id) REFERENCES Positions(position_id),
    FOREIGN KEY (player_id) REFERENCES Players(player_id)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Teams (
    team_id INTEGER PRIMARY KEY,
    team_name TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Positions (
    position_id INTEGER,
    position TEXT,
    from_time TEXT,
    to_time TEXT,
    from_period INTEGER,
    to_period INTEGER,
    start_reason TEXT,
    end_reason TEXT,
    PRIMARY KEY (position_id, player_id, team_id)
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



conn.commit()




# df = pd.read_json('15946.json')

# Vie DataFrame SQL-tietokantaan
# filtered_positions = positions[['team_id', 'team_name']]
# filtered_positions.columns = ['team_id', 'team_name']  # Varmista oikeat sarakenimet

# teams = df[['team_id', 'team_name']].drop_duplicates()
player_positions.to_sql('PlayerPositions', conn, if_exists='replace', index=False)
teams.to_sql('Teams', conn, if_exists='replace', index=False)

players.to_sql('Players', conn, if_exists='replace', index=False)

# positions = df[['position_id', 'position', 'player_id', 'team_id', 'from_time', 'to_time', 'from_period', 'to_period', 'start_reason', 'end_reason']].drop_duplicates()
positions.to_sql('Positions', conn, if_exists='replace', index=False)

cards.to_sql('Cards', conn, if_exists='replace', index=False)

countries.to_sql('Countries', conn, if_exists='replace', index=False)



# Tarkista tietokannan sisältö
results_player_positions = pd.read_sql('SELECT * FROM PlayerPositions', conn)
# print(results_player_positions)


results_teams = pd.read_sql('SELECT * FROM Teams', conn)
# print(results_teams)

results_positions = pd.read_sql('SELECT * FROM Positions', conn)
pd.set_option('display.max_columns', None)
# print(results_positions)

results_players = pd.read_sql('SELECT * FROM Players', conn)
pd.set_option('display.max_columns', None)
# print(results_players)

results_cards = pd.read_sql('SELECT * FROM Cards', conn)
pd.set_option('display.max_columns', None)
# print(results_cards)

results_countries = pd.read_sql('SELECT * FROM Countries', conn)
pd.set_option('display.max_columns', None)
# print(results_countries)





