import facebook
import datetime
import sqlite3
import csv

def getReactions(reacts):
    all_reacts = []
    for i in reacts:
        r = {
            'name': i['name'],
            'reaction' : i['type']
        }
        all_reacts.append(r)
    return all_reacts

def getPostID(pid):
    pid = pid.split('_')[1]
    return (pid)

def posts_count(uid):
    doc = uwps.find_one({"_id" : uid})
    cnt = 0
    if not doc:
        return cnt;
    else:
        return doc['Posts']

def getReactionsCount(reacts):
    r = ['LIKE', 'LOVE', 'HAHA', 'WOW', 'SAD', 'ANGRY']
    rnum = [0] * 6

    for i in reacts:
        index = r.index(i['type'])
        rnum[index] += 1
    return rnum


def insertPostStats(postID, ownerInfo, date, reacts):
    '''
        Post wise stats
    '''
    rnum = getReactionsCount(reacts)

    uid = ownerInfo['ID']
    uname = ownerInfo['Name']

    params = (postID, date, uid, uname, rnum[0], rnum[1], rnum[2], rnum[3], rnum[4], rnum[5])
    c.execute("INSERT OR IGNORE INTO pws VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", params)


def updateUserPostStats(ownerInfo, reacts):
    '''
        User wise stats(posts)
    '''
    rnum = getReactionsCount(reacts)

    uid = ownerInfo['ID']
    uname = ownerInfo['Name']

    c.execute("UPDATE uwps SET \
            Posts = Posts + 1, \
            Likes = Likes + " + str(rnum[0])
            + ", Loves = Loves + " + str(rnum[1])
            + ", Hahas = Hahas + " + str(rnum[2])
            + ", Wows = Wows + " + str(rnum[3])
            + ", Sads = Sads + " + str(rnum[4])
            + ", Angrys = Angrys + " + str(rnum[5])
            + " WHERE UserID = " + str(uid)
            )

    params = (uid, uname, 1, rnum[0], rnum[1], rnum[2], rnum[3], rnum[4], rnum[5])
    c.execute("INSERT OR IGNORE INTO uwps VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", params)


def updateUserReactionStats(reacts):
    '''
        User wise stats(reactions)
    '''
    r = ['LIKE', 'LOVE', 'HAHA', 'WOW', 'SAD', 'ANGRY']

    for i in reacts:
        rnum = [0] * 6
        uid = i['id']
        uname = i['name']
        index = r.index(i['type'])
        rnum[index] += 1

        c.execute("UPDATE uwrs SET \
            Likes = Likes + " + str(rnum[0])
            + ", Loves = Loves + " + str(rnum[1])
            + ", Hahas = Hahas + " + str(rnum[2])
            + ", Wows = Wows + " + str(rnum[3])
            + ", Sads = Sads + " + str(rnum[4])
            + ", Angrys = Angrys + " + str(rnum[5])
            + " WHERE UserID = " + str(uid)
            )

        params = (uid, uname, rnum[0], rnum[1], rnum[2], rnum[3], rnum[4], rnum[5])
        c.execute("INSERT OR IGNORE INTO uwrs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", params)


def insertIntoDB(post):
    date = post['created_time']
    postID = getPostID(post['id'])
    ownerInfo = {
            "Name" : post['from']['name'],
            "ID" : post['from']['id']
    }

    try:
        reacts = post['reactions']['data']
    except KeyError:
        reacts = []

    insertPostStats(postID, ownerInfo, date, reacts)
    updateUserPostStats(ownerInfo, reacts)
    updateUserReactionStats(reacts)


def getDetails(GROUP_NAME, posts):
    for post in posts:
        insertIntoDB(post)

    with open(GROUP_NAME+'_pws.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for row in c.execute("SELECT * FROM pws"):
            writer.writerow(row)
    print("Post-wise stats generated in <GROUP_NAME>_pws.csv")

    with open(GROUP_NAME+'_uwps.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for row in c.execute("SELECT * FROM uwps"):
            writer.writerow(row)
    print("User-wise post stats generated in <GROUP_NAME>_uwps.csv")

    with open(GROUP_NAME+'_uwrs.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for row in c.execute("SELECT * FROM uwrs"):
            writer.writerow(row)
    print("User-wise reaction stats generated in <GROUP_NAME>_uwrs.csv")


if __name__ == '__main__':
    #ACCESS_TOKEN = str(raw_input("Access token: "))
    #GROUP_ID = str(raw_input("Group ID: "))

    with open('details.txt', 'r') as f:
        ACCESS_TOKEN = f.readline()
        GROUP_ID = f.readline()

    graph = facebook.GraphAPI(ACCESS_TOKEN, version='2.7')
    events = graph.get_object(id=GROUP_ID, fields='name, description, owner, feed.limit(10000){created_time, from, reactions, permalink_url}')

    db_name = str(events['name'] + '_' + GROUP_ID)
    all_posts = events['feed']['data']

    conn = sqlite3.connect(db_name[:-1])
    print("Database generated in <GROUP_NAME>_<GROUP_ID>")
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS pws
            (PostID TEXT PRIMARY KEY NOT NULL,
            Date TEXT NOT NULL,
            ownerID TEXT NOT NULL,
            ownerName TEXT NOT NULL,
			Likes INTEGER NOT NULL,
			Loves INTEGER NOT NULL,
			Hahas INTEGER NOT NULL,
			Wows INTEGER NOT NULL,
			Sads INTEGER NOT NULL,
			Angrys INTEGER NOT NULL)
            ''')

    c.execute("DROP TABLE IF EXISTS uwrs")
    c.execute('''CREATE TABLE IF NOT EXISTS uwrs
            (UserID TEXT PRIMARY KEY NOT NULL,
            Name TEXT NOT NULL,
			Likes INTEGER NOT NULL,
			Loves INTEGER NOT NULL,
			Hahas INTEGER NOT NULL,
			Wows INTEGER NOT NULL,
			Sads INTEGER NOT NULL,
			Angrys INTEGER NOT NULL)
            ''')

    c.execute("DROP TABLE IF EXISTS uwps")
    c.execute('''CREATE TABLE IF NOT EXISTS uwps
            (UserID TEXT PRIMARY KEY NOT NULL,
            Name TEXT NOT NULL,
            Posts INTEGER NOT NULL,
			Likes INTEGER NOT NULL,
			Loves INTEGER NOT NULL,
			Hahas INTEGER NOT NULL,
			Wows INTEGER NOT NULL,
			Sads INTEGER NOT NULL,
			Angrys INTEGER NOT NULL)
            ''')

    getDetails(events['name'], all_posts)

    conn.commit()
    conn.close()
