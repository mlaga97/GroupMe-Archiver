import urllib.request, json, time, requests, sqlite3

# Load Configuration
print('Loading configuration...');
with open('config.json') as f:
    config = json.load(f);
    token = config['token'];
    dbPath = config['dbPath'];
    userLUT = config['users'];
    groupID = config['groupID'];
    limit = config['requestSize'];
print('Done!');

# Open Database
print();
print('Connecting to DB...');
con = sqlite3.connect(dbPath);
cur = con.cursor();
print('Done!');

print();
print('Checking database configuration...');
cur.execute('CREATE TABLE IF NOT EXISTS "group_' + groupID + '" ( `messageID` INTEGER, `messageJSON` TEXT, PRIMARY KEY(`messageID`) )');
print('Done!');

# Globals
done = False
suffix = ''
staleCache = {}

################################################################################
################################################################################
################################################################################
# Helper Functions

def getPage():
    if suffix == '':
        print('  Requesting new page from GroupMe...');
    else:
        print('  Requesting new page from GroupMe starting with message ' + str(suffix));

    # Retrieve Messages
    requestURL = 'https://api.groupme.com/v3/groups/' + groupID + '/messages?token=' + token + '&limit=' + str(limit) + suffix;
    r = requests.get(requestURL);
    data = r.json()
    messages = data['response']['messages']
    print('  Done!');
    return messages;

################################################################################
################################################################################
################################################################################
# Get List of Existing Messages

print();
print('Retrieving list of cached messages from DB...');
minID = 0;
maxID = 0;
for row in cur.execute('SELECT messageID, messageJSON FROM group_' + groupID):
    messageID = row[0];
    messageJSON = row[1];
    messageData = json.loads(messageJSON);

    timestamp = messageData['created_at'];
    likes = messageData['favorited_by'];

    if minID == 0:
        minID = messageID;

    if maxID == 0:
        maxID = messageID;

    if messageID < minID:
        minID = messageID;

    if messageID > maxID:
        maxID = messageID;

    staleCache[messageID] = {
        'timestamp': timestamp,
        'likes': likes,
    }
print('Done!');
print();
print('Min messageID in DB: ' + str(minID));
print('Max messageID in DB: ' + str(maxID));

################################################################################
################################################################################
################################################################################
# Retrieve New Messages

print()
print('Retrieving new messages from GroupMe...');
while not done:
    messages = getPage();

    # Add Messages to Database
    print();
    print('  Parsing messages...')
    for message in messages:
        messageID = message['id'];
        messageJSON = json.dumps(message);

        if int(messageID) <= int(maxID):
            print('    Message ' + str(messageID) + ' already cached in DB!');
            done = True;
            break;
        else:
            cur.execute('INSERT INTO group_' + groupID + ' VALUES (?, ?);', (messageID, messageJSON));
            suffix = '&before_id=' + str(messages[limit-1]['id']); # TODO
            print('    Got new message ' + str(messageID) + ' and cached in DB!');

    con.commit();
    print('  Done!');

con.commit();
print('Done!');

################################################################################
################################################################################
################################################################################
# Go back a little farther to check likes

print()
print('Retrieving stale messages from GroupMe');
done = False;
countWithoutUpdates = 0;

while not done:
    flag = False;
    messages = getPage();

    # Add Messages to Database
    print();
    print('  Parsing messages...')
    for message in messages:
        messageID = int(message['id']);
        messageJSON = json.dumps(message);

        if str(staleCache[messageID]['likes']) != str(message['favorited_by']):
            print('    Likes changed for ' + str(messageID) + '!');
            cur.execute('UPDATE group_' + groupID + ' SET messageJSON = ? WHERE messageID = ?;', (messageJSON, messageID));
            flag = True;

        suffix = '&before_id=' + str(messages[limit-1]['id']); # TODO
        con.commit();

    # Increment update counter
    if flag:
        countWithoutUpdates = 0;
    else:
        countWithoutUpdates = countWithoutUpdates + 1;

    # Only check back 1500 messages
    if countWithoutUpdates > 15:
        done = True;

    con.commit();
    print('  Done!');
    print();

    if done:
        print('  Went too long without any changes. Moving on!');

con.commit();
print('Done!');

################################################################################
################################################################################
################################################################################
# Retrieve Backlog Messages

print()
print('Retrieving backlog messages from GroupMe...');
done = False;
suffix = '&before_id=' + str(minID);

while not done:
    print('  Requesting new page from GroupMe starting with message ' + str(suffix));

    # Retrieve Messages
    requestURL = 'https://api.groupme.com/v3/groups/' + groupID + '/messages?token=' + token + '&limit=' + str(limit) + suffix;
    r = requests.get(requestURL);

    # Check to see if we're done
    if r.status_code == 304:
        done = True
        print("    Looks like that's the end of the line!");
        print('  Done!')
        continue

    # Keep going otherwise
    data = r.json()
    messages = data['response']['messages']

    # Add Messages to Database
    print('  Parsing messages...')
    for message in messages:
        messageID = message['id'];
        messageJSON = json.dumps(message);

        if int(messageID) >= int(minID):
            print('    Message ' + str(messageID) + ' already cached in DB!');
        else:
            cur.execute('INSERT INTO group_' + groupID + ' VALUES (?, ?);', (messageID, messageJSON));
            suffix = '&before_id=' + str(messages[limit-1]['id']); # TODO
            print('    Got new message ' + str(messageID) + ' and cached in DB!');

    if len(messages) < limit:
        done = True;
    
    con.commit();
    print('  Done!');

con.commit();
print('Done!');

# Clean up
print();
print('Cleaning up...');
con.commit();
con.close();
print('Done!'); 
exit();
