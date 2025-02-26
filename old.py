import requests, json, sys, pyodbc


#Fetch data from MLB Stats API
url = 'https://statsapi.mlb.com/api/v1/sports/1/players'
response = requests.get(url)
data = response.json()


#teams = data.get('people', [])



#write to json file
with open('mlb_teams.json', 'w') as file:
    json.dump(data, file, indent=4)


# SQL Server connection
# server ='SHARKBAIT-DT\MSSQLSERVER01'
# database = 'MLBStats'
# driver='{ODBC Driver 17 for SQL Server}'

# connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# sql_insert_teams = 'INSERT INTO MLB_Teams (team_id, name, league, venue, abbreviation, first_year, division) VALUES (?, ?, ?, ?, ?, ?, ?)'


# with pyodbc.connect(connection_string) as conn:
#     cursor = conn.cursor()

#     for team in teams:
#         if team['sport']['id'] == 1:
#             team_id = team['id']
#             name = team['name']
#             league = team['league']['name'] if 'league' in team else 'N/A'
#             venue = team['venue']['name'] if 'venue' in team else 'N/A'
#             abbreviation = team['abbreviation']
#             firstYearOfPlay = team['firstYearOfPlay']
#             division = team['division']['name']
#             try:
#                 cursor.execute(
#                         sql_insert_teams,
#                         (team_id, name, league, venue, abbreviation, firstYearOfPlay, division)
#                     )
                
#             except Exception as e:
#                 print(f'Error inserting {name}: {e}')

#     conn.commit()
#     cursor.close()

if __name__ == '__main__':
    pass

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