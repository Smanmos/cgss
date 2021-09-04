import sqlite3
import csv
from io import StringIO
import pandas
import datetime

def combo_multiplier(note, total):
    if note >= total * 9 // 10:
        return 2
    elif note >= total * 8 // 10:
        return 1.7
    elif note >= total * 7 // 10:
        return 1.5
    elif note >= total // 2:
        return 1.4
    elif note >= total // 4:
        return 1.3
    elif note >= total // 10:
        return 1.2
    elif note >= total // 20:
        return 1.1
    else:
        return 1

def active(time, period, uptime):
    return time >= period and time % period < uptime

def is_flick(note):
    return note['status'] == 1 or note['status'] == 2

def is_slide(note):
    return note['type'] == 3

long_end = [False] * 5
def is_long(note):
    pos = int(note['finishPos']) - 1
    if note['type'] == 2:
        long_end[pos] = not long_end[pos]
        return True
    elif is_flick(note) and long_end[pos]:
        long_end[pos] = False
        return True
    else:
        return False



master_con = sqlite3.connect('chihiro/data/db/master.db')
cur = master_con.cursor()
diff_id = [4, 5, 101]
diff_names = {4: "MASTER", 5: "MASTER+", 101: "LEGACY"}
types = {1: "Cu", 2: "Co", 3: "Pa", 4: "All"}
req = cur.execute(  'SELECT ld.id, music_data.name, live_detail.difficulty_type, live_detail.level_vocal, ld.type '
                    'FROM music_data '
                    'INNER JOIN live_data AS ld '
                    'ON ld.music_data_id = music_data.id '
                    'INNER JOIN live_detail '
                    'ON ld.id = live_detail.live_data_id '
                    'AND live_detail.difficulty_type IN (' + ', '.join(map(str,diff_id)) + ') '
                    'WHERE ( NOT EXISTS ('
                    'SELECT other.id '
                    'FROM live_data AS other, music_data md1, music_data md2 '
                    'WHERE other.id < ld.id '
                    'AND other.difficulty_5 <> 0 '
                    'AND other.music_data_id = md1.id '
                    'AND ld.music_data_id = md2.id '
                    'AND md1.name = md2.name))')

now = datetime.datetime.now()
print(now.strftime('%Y%m%d-%H%M%S'))

#data = pandas.DataFrame(index = ['Song Name', 'Notes'])
data = []
timers = [[3, 4], [4.5, 7], [6, 9], [7.5, 11], [9, 13], [4.5, 6], [6, 7], [7.5, 9], [9, 11], [7.5, 12]]
act_timers = [[6, 7], [7.5, 9], [9, 11]]
note_types = ['long', 'flick', 'slide']
verifier = {'long': is_long, 'flick': is_flick, 'slide': is_slide}

with open('level_data.csv', 'w', encoding = 'utf-8') as fp:
    myfile = csv.writer(fp)
    for level_data in req:
        song_id = level_data[0]
        song_name = level_data[1]
        diff_type = level_data[2]
        diff = level_data[3]
        song_type = level_data[4]
        if song_id >= 1000:
            continue
        score_db = 'chihiro/data/musicscores/musicscores_m{:03d}.db'.format(song_id)
        score_con = sqlite3.connect(score_db)
        score_cur = score_con.cursor()
        print(song_id, diff_type)
        score_name = 'musicscores/m{:03d}/{}_{}.csv'.format(song_id, song_id, diff_type)
        try:
            score_req = score_cur.execute("SELECT data FROM blobs WHERE name = ?", (score_name,))
            score = None
            for s in score_req:
                # print('found score')
                enc_score = s[0]
                score = enc_score.decode('utf-8')
                score_ifile = StringIO(score)
                score_data = pandas.read_csv(score_ifile)
                if song_id == 1:
                    print(score_data)
                note_count = score_data.shape[0] - 3
                song_data = {'Song Name': song_name, 'Song id': song_id, 'Notes': note_count, 'Difficulty': diff_names[diff_type], 'Type': types[song_type], 'Level': diff}
                skill_uptime = [0] * len(timers)
                act_skill_uptime = {}
                for type in note_types:
                    act_skill_uptime[type] = [0] * len(act_timers)
                # Problematic loop?
                skip_notes = 0
                for index, note in score_data.iterrows():
                    if note['type'] <= 3:
                        for idx, timer in enumerate(timers):
                            if active(note['sec'], timer[1], timer[0]):
                                skill_uptime[idx] += combo_multiplier(note['id'] - skip_notes, note_count)
                        for type in note_types:
                            if verifier[type](note):
                                for idx, timer in enumerate(act_timers):
                                    if active(note['sec'], timer[1], timer[0]):
                                        act_skill_uptime[type][idx] += combo_multiplier(note['id'] - skip_notes, note_count)
                    else:
                        skip_notes += 1
                        if note['type'] == 100:
                            note_count = int(note['status'])
                            song_data['Notes'] = note_count

                for index, timer in enumerate(timers):
                    song_data['{}/{}s'.format(timer[0], timer[1])] = skill_uptime[index] / note_count / 1.41
                for type in note_types:
                    for index, timer in enumerate(act_timers):
                        song_data['{}/{}s {}'.format(timer[0], timer[1], type)] = act_skill_uptime[type][index] / note_count / 1.41
                data.append(song_data)
        except sqlite3.OperationalError:
            print('File ' + str(song_id) + ' not found')

processed_data = pandas.DataFrame(data)
now = datetime.datetime.now()
print(now.strftime('%Y%m%d-%H%M%S'))
processed_data.to_csv('level_data' + now.strftime('%Y%m%d-%H%M%S') + '.csv')
