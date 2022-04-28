import urllib.request, json, time, requests, sqlite3

# Load Configuration
print('Loading configuration...');
with open('config.json') as f:
    config = json.load(f);
    token = config['token'];
    dbPath = config['dbPath'];
    userLUT = config['users'];
    groupID = config['groupID'];
print('Done!');

# Open Database
print();
print('Connecting to DB...');
con = sqlite3.connect(dbPath);
cur = con.cursor();
print('Done!');

print();
print('Checking database configuration...');
cur.execute('CREATE TABLE IF NOT EXISTS "polls_' + groupID + '" ( `pollID` INTEGER, `pollJSON` TEXT, PRIMARY KEY(`pollID`) )');
print('Done!');

# Globals
polls = [];
pollCache = {}

# Get cached polls from DB
print();
print('Retrieving and parsing cached polls from DB...');
for row in cur.execute('SELECT * FROM polls_' + groupID):
    pollID = row[0];
    pollJSON = row[1];
    pollData = json.loads(pollJSON);

    pollCache[pollID] = pollData;

# Get List of Existing Messages
print();
print('Retrieving and parsing cached messages from DB...');
for row in cur.execute('SELECT * FROM group_' + groupID):
    messageID = row[0];
    messageJSON = row[1];
    messageData = json.loads(messageJSON);

    # Attachments
    for attachment in messageData['attachments']:
        if attachment['type'] == 'poll':
            pollID = attachment['poll_id'];

            if int(pollID) not in pollCache:
                print('  Poll ' + pollID + ' not in cache!');
                polls.append(pollID);
print('Done!');

# Download Polls
print();
print('Retrieving new polls from GroupMe...');
for pollID in polls:
    print('  Retrieving poll ' + pollID + ' from GroupMe...');
    requestURL = 'https://api.groupme.com/v3/poll/' + groupID + '/' + pollID + '?token=' + token;
    r = requests.get(requestURL);
    data = r.json()
    pollData = data['response']['poll']['data'];
    print('  Done!');

    print('  Inserting poll ' + pollID + ' into cache...');
    pollJSON = json.dumps(pollData);
    cur.execute('INSERT INTO polls_' + groupID + ' VALUES (?, ?);', (pollID, pollJSON));
    print('  Done!');
print('Done!');

print();
print('Updating any unfinished polls...');
for pollID in pollCache:
    pollData = pollCache[pollID];
    status = pollData['status'];

    if status == 'past':
        a = 1;
    else:
        print('  Poll ' + str(pollID) + ' is still active!');

        print('  Retrieving poll ' + str(pollID) + ' from GroupMe...');
        requestURL = 'https://api.groupme.com/v3/poll/' + groupID + '/' + str(pollID) + '?token=' + token;
        r = requests.get(requestURL);
        data = r.json()
        pollData = data['response']['poll']['data'];
        print('  Done!');

        print('  Updating poll ' + str(pollID) + '...');
        cur.execute('UPDATE polls_' + str(groupID) + ' SET pollJSON = ? WHERE pollID = ?;', (pollJSON, pollID));
        print('  Done!');

print('Done!');

# Clean up
print();
print('Cleaning up...');
con.commit();
con.close();
print('Done!'); 
exit();
