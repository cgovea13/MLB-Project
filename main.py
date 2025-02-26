import requests, json, sys, pyodbc


class APIConnection:
    def __init__(self, obj, input, sql_conn):
        #Fetch data from MLB Stats API
        self.obj = obj
        self.url = f'https://statsapi.mlb.com/api/v1/{obj.endpoint}'
        self.response = requests.get(self.url)
        self.data = self.response.json()
        self.json_data = self.data.get(obj.key, [])
        self.sql = obj.sql
        self.input = input
        self.sql_conn = sql_conn

    def insert(self):
        #if input is 'team' then Team().insertTeams()
        if self.input == 'team':
            self.obj.insertTeams(self.sql_conn, self.json_data)
        elif self.input == 'player':
            self.obj.insertPlayers(self.sql_conn, self.json_data)
        
        
class Team:
    def __init__(self):
        #teams = data.get('teams', [])
        self.endpoint = 'teams'
        self.key = 'teams'
        self.sql = '''INSERT INTO Teams (team_id, name, league, venue, abbreviation, first_year, division) VALUES (?, ?, ?, ?, ?, ?, ?)'''
        self.team_id = None
        self.name = None
        self.league = None
        self.venue = None
        self.abbreviation = None
        self.firstYearOfPlay = None
        self.division = None

    #PASS SQL CONNECTION
    def insertTeams(self, sql_conn, json_data):
        with pyodbc.connect(sql_conn.connection_string) as conn:
            cursor = conn.cursor()
            for team in json_data:
                if team['sport']['id'] == 1:
                    self.team_id = team['id']
                    self.name = team['name']
                    self.league = team['league']['name'] if 'league' in team else 'N/A'
                    self.venue = team['venue']['name'] if 'venue' in team else 'N/A'
                    self.abbreviation = team['abbreviation']
                    self.firstYearOfPlay = team['firstYearOfPlay']
                    self.division = team['division']['name']
            # print(self.api.sql)
            #print(self.api.name)
                    try:
                        cursor.execute(
                                        self.sql,
                                        (self.team_id, self.name, self.league, self.venue, self.abbreviation, self.firstYearOfPlay, self.division)
                                    )
                                
                    except Exception as e:
                        print(f'Error inserting {self.name}: {e}')          

            conn.commit()
            cursor.close()
    
class Player:
    def __init__(self):
        self.endpoint = 'sports/1/players'
        self.key = 'people'
        self.sql = '''INSERT INTO Players (player_id, first_name, last_name, primary_number, birth_date, birth_city, birth_country, height,
            weight, active, current_team, primary_position, bat_side, pitch_hand, strike_zone_top, strike_zone_bottom) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        self.player_id, self.first_name, self.last_name, self.primary_number, self.birth_date, self.birth_city, self.birth_country, self.height, self.weight = None, None, None, None, None, None, None, None, None
        self.active, self.current_team, self.primary_position, self.bat_side, self.pitch_hand, self.strike_zone_top, self.strike_zone_bottom = None, None, None, None, None, None, None

    def insertPlayers(self, sql_conn, json_data):
        with pyodbc.connect(sql_conn.connection_string) as conn:
            cursor = conn.cursor()
            for player in json_data:
                self.player_id = player['id']
                self.first_name = player['firstName']
                self.last_name = player['lastName']
                self.primary_number = player['primaryNumber'] if 'primaryNumber' in player else 'N/A'
                self.birth_date = player['birthDate']
                self.birth_city = player['birthCity'] if 'birthCity' in player else 'N/A'
                self.birth_country = player['birthCountry']
                self.height = player['height']
                self.weight = player['weight']
                self.active = player['active']
                self.current_team = player['currentTeam']['id'] if 'currentTeam' in player else 'N/A'
                self.primary_position = player['primaryPosition']['name'] if 'primaryPosition' in player else 'N/A'
                self.bat_side = player['batSide']['description'] if 'batSide' in player else 'N/A'
                self.pitch_hand = player['pitchHand']['description'] if 'pitchHand' in player else 'N/A'
                self.strike_zone_top = player['strikeZoneTop']
                self.strike_zone_bottom = player['strikeZoneBottom']
                try:
                        cursor.execute(
                                        self.sql,
                                        (self.player_id, self.first_name, self.last_name, self.primary_number, self.birth_date, self.birth_city, self.birth_country, self.height,
                                        self.weight, self.active, self.current_team, self.primary_position, self.bat_side, self.pitch_hand, self.strike_zone_top, self.strike_zone_bottom)
                                    )
                                
                except Exception as e:
                    print(f'Error inserting {self.name}: {e}')  
                
            conn.commit()
            cursor.close()

# team = Team()
# print(team.sql_insert_teams)
# api = APIConnection(team)
# sqlconn = SQLConnection(api)
#print(api.update_object)

class SQLConnection:
    def __init__(self):
    # SQL Server connection
        self.server ='SHARKBAIT-DT\\MSSQLSERVER01'
        self.database = 'MLBStats'
        self.driver='{ODBC Driver 17 for SQL Server}'
        self.connection_string = f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;'
        #self.json_data = api.json_data
        #self.api = api
        
        
    

    def insertPlayers(self):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            for team in self.api.json_data:
                self.team_id = team['id']
                self.name = team['fullName']
            # print(self.api.sql)
            #print(self.api.name)
                try:
                    cursor.execute(
                                    self.api.sql,
                                    (self.team_id, self.name)
                                )
                            
                except Exception as e:
                    print(f'Error inserting {self.name}: {e}')    

if __name__ == '__main__':
    #maybe take input to check waht user wants to update?
    #team = Team()
    # player = Player()
    # api = APIConnection(player)
    # sqlconn = SQLConnection(api)
    # #sqlconn.api.json_data
    # sqlconn.insertPlayers()
    update_table = input('Enter Player or Team\n').strip().lower()
    sql_conn = SQLConnection()
    #team = Team()
    player = Player()
    api = APIConnection(player, update_table, sql_conn)
    api.insert()
    


#print(json.dumps(mlb_teams_nested, indent = 4))
#write to json file
# with open('mlb_teams.json', 'w') as file:
#     json.dump(data, file, indent=4)

    #get all fields into dict comprehension
    # mlb_teams = [team for team in teams if team["sport"]["id"] == 1]
    # mlb_teams_dict = {team["id"]: team for team in teams if team["sport"]["id"] == 1}

    #get nested fields into dict comprehension
    # mlb_teams_nested = {
    #     team['id']: {
    #         'name': team['name'],
    #         'league': team['league']['name'] if 'league' in team else 'N/A',
    #         #'spring_league': team['springLeague']['name'] if 'springLeague' in team else 'N/A',
    #         'venue': team['venue']['name'] if 'venue' in team else 'N/A',
    #         'abbreviation': team['abbreviation'],
    #         'firstYearOfPlay': team['firstYearOfPlay'],
    #         'division': team['division']['name']
    #     }
    #     for team in teams if team['sport']['id'] == 1
    # }