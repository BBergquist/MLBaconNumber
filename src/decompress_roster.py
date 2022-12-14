'''
Script for converting the Rosters.zip file to a single CSV.
'''

import os
import zipfile

ZIP_FILE = '/workspaces/MLBaconNumber/data/Rosters.zip'
ROSTERS_DIR = '/workspaces/MLBaconNumber/data/Rosters/'
ALL_ROSTERS = ROSTERS_DIR + "all_rosters.csv"
COLUMN_NAMES = ["year", "player_id", "last", "first", "bats", "throws", "team_id", "position"]


def append_roster(out_file, roster_file):
    '''
    Reads a roster file and appends it to the out_file.
    '''

    year = roster_file[3:7]
    full_path = ROSTERS_DIR + roster_file
    with open(full_path, 'r') as roster:
        _ = [out_file.write(year + "," + player)
             for player in roster if len(player) > 1]


with zipfile.ZipFile(ZIP_FILE, 'r') as zipped:
    zipped.extractall(ROSTERS_DIR)

roster_paths = os.listdir(ROSTERS_DIR)
with open(ALL_ROSTERS, 'w') as out:
    out.write(','.join(COLUMN_NAMES) + '\n')
    for roster_path in roster_paths:
        append_roster(out, roster_path)
