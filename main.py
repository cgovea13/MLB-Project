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
        self.endpoint = 'teams'
        self.key = 'teams'
        self.trunc_sql = 'TRUNCATE TABLE Teams'
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
        print('Inserting Teams')
        with pyodbc.connect(sql_conn.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(self.trunc_sql)
            for team in json_data:
                if team['sport']['id'] == 1:
                    self.team_id = team['id']
                    self.name = team['name']
                    self.league = team['league']['name'] if 'league' in team else 'N/A'
                    self.venue = team['venue']['name'] if 'venue' in team else 'N/A'
                    self.abbreviation = team['abbreviation']
                    self.firstYearOfPlay = team['firstYearOfPlay']
                    self.division = team['division']['name']
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
        self.trunc_sql = 'TRUNCATE TABLE Players'
        self.sql = '''INSERT INTO Players (player_id, first_name, last_name, primary_number, birth_date, birth_city, birth_country, height,
            weight, active, current_team, primary_position, bat_side, pitch_hand, strike_zone_top, strike_zone_bottom) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        
        self.player_id, self.first_name, self.last_name, self.primary_number, self.birth_date, self.birth_city, self.birth_country, self.height, self.weight = None, None, None, None, None, None, None, None, None
        self.active, self.current_team, self.primary_position, self.bat_side, self.pitch_hand, self.strike_zone_top, self.strike_zone_bottom = None, None, None, None, None, None, None
       
        self.id_list = []

        self.sql_conn = None

    def insertPlayers(self, sql_conn, json_data):
        print('Inserting Players')
        self.sql_conn = sql_conn
        with pyodbc.connect(self.sql_conn.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN TRANSACTION')
            cursor.execute(self.trunc_sql)
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
                        #conn.commit()
                        self.id_list.append(self.player_id)

                except Exception as e:
                    print(f'Error inserting {self.first_name}: {e}')  
                    cursor.execute('ROLLBACK TRANSACTION')
            
            cursor.execute('COMMIT TRANSACTION')
            cursor.close()
        
        #self.insertHittingStats()

    def insertHittingStats(self):
        #SQL hitting stats
        trunc_sql = 'TRUNCATE TABLE Player_Stats'
        sql = '''INSERT INTO Player_Stats (Player_ID, Games_Played, Ground_Out, Air_Outs, Runs, Doubles, Triples, Home_Runs, Strike_Outs,
	        Base_On_Balls, Intentional_Walks, Hits, Hit_By_Pitch, Average, At_Bats, OBP, SLG, OPS, Caught_Stealing, Stolen_Bases, Stolen_Base_Percentage) 
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        #hitting stats
        player_id, Games_Played, Ground_Out,Air_Outs, Runs, Doubles, Triples, Home_Runs = None, None, None, None, None, None, None, None
        Strike_Outs, Base_On_Balls, Intentional_Walks, Hits, Hit_By_Pitch, Average = None, None, None, None, None, None
        At_Bats, OBP, SLG, OPS, Caught_Stealing, Stolen_Bases, Stolen_Base_Percentage = None, None, None, None, None, None, None

        with pyodbc.connect(self.sql_conn.connection_string) as conn:
            print('Inserting Player Stats')
            cursor = conn.cursor()
            cursor.execute(trunc_sql)
            counter = 0
            for id in self.id_list:
                counter += 1
                print(f'Inserting {counter}/{len(self.id_list)}')

                endpoint = f'https://statsapi.mlb.com/api/v1/people/{id}/stats?stats=career&group=hitting'
                response = requests.get(endpoint)
                data = response.json()
                #json_data = data.get('stats', [])
                if not data.get('stats'):
                    continue
                stats = data['stats'][0]['splits']
                if not stats:
                    continue
                stat_values = stats[0]['stat']
                player_id = id
                Games_Played = stat_values['gamesPlayed']
                Ground_Out = stat_values['groundOuts']
                Air_Outs = stat_values['airOuts']
                Runs = stat_values['runs']
                Doubles = stat_values['doubles']
                Triples = stat_values['triples']
                Home_Runs = stat_values['homeRuns']
                Strike_Outs = stat_values['strikeOuts']
                Base_On_Balls = stat_values['baseOnBalls']
                Intentional_Walks = stat_values['intentionalWalks']
                Hits = stat_values['hits']
                Hit_By_Pitch = stat_values['hitByPitch']
                Average = stat_values['avg']
                At_Bats = stat_values['atBats']
                OBP = stat_values['obp']
                SLG = stat_values['slg']
                OPS = stat_values['ops']
                Caught_Stealing = stat_values['caughtStealing']
                Stolen_Bases = stat_values['stolenBases']
                Stolen_Base_Percentage = stat_values['stolenBasePercentage']
                try:
                        cursor.execute(
                                        sql,
                                        (player_id, Games_Played, Ground_Out, Air_Outs, Runs, Doubles, Triples, Home_Runs, Strike_Outs,
	                                    Base_On_Balls, Intentional_Walks, Hits, Hit_By_Pitch, Average, At_Bats, OBP, SLG, OPS, Caught_Stealing, Stolen_Bases, Stolen_Base_Percentage)
                                    )
                        
                        self.id_list.append(self.player_id)

                except Exception as e:
                    print(f'Error inserting {self.first_name}: {e}') 
                
                conn.commit()
            cursor.close()

class SQLConnection:
    def __init__(self):
    # SQL Server connection
        self.server ='SHARKBAIT-DT\\MSSQLSERVER01'
        self.database = 'MLBStats'
        self.driver='{ODBC Driver 17 for SQL Server}'
        self.connection_string = f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;'
        #self.json_data = api.json_data
        #self.api = api
         

if __name__ == '__main__':

    #update_table = input('Enter Player or Team\n').strip().lower()
    update_table = 'player'
    sql_conn = SQLConnection()
    #team = Team()
    player = Player()
    api = APIConnection(player, update_table, sql_conn)
    api.insert()
    player.insertHittingStats()
    


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