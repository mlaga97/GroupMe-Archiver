import sqlite3, json, csv
from datetime import datetime

# Load Configuration
print('Loading configuration...');
with open('config.json') as f:
    config = json.load(f);
    groupID = config['groupID'];
    userLUT = config['users'];
    dbPath = config['dbPath'];
    outputDir = config['outputDir'];
print('Done!');

# Open Database
print();
print('Connecting to DB...');
con = sqlite3.connect(dbPath);
cur = con.cursor();
print('Done!');

# Globals
users = {}

def printMessage(messageData):
    print('================================================================================');
    print(messageData['name'] + ' (' + userLUT[messageData['sender_id']] + ')');
    print()
    print(messageData['text']);
    print()

    for attachment in messageData['attachments']:
        if attachment['type'] == 'image':
            print("Image: " + attachment['url']);
        elif attachment['type'] == 'linked_image':
            print("Image: " + attachment['url']);
            continue;
        elif attachment['type'] == 'video':
            print("Video: " + attachment['url']);
            continue;
        elif attachment['type'] == 'poll':
            continue;
        elif attachment['type'] == 'file':
            continue;
        elif attachment['type'] == 'emoji':
            continue;
        elif attachment['type'] == 'reply':
            continue;
        elif attachment['type'] == 'mentions':
            continue;
        elif attachment['type'] == 'postprocessing':
            continue;
        else:
            print(attachment);

    print(str(datetime.fromtimestamp(messageData['created_at'])));
    print('================================================================================');

def addUser(userID):
    users[userID] = {
        'names': [],
        'avatars': [],
        'messages': 0,
        'attachments': {
            'images': 0,
            'linked_images': 0,
            'videos': 0,
            'polls': 0,
            'files': 0,
            'emojis': 0,
            'replies': 0,
            'mentions': 0,
        },
        'likedBy': {},
        'selfLikes': 0,
        'totalLikesGiven': 0,
        'totalLikesReceived': 0,
        'lastPost': 0,
    };

def addUserIfNotPresent(userID):
    if userID not in users:
        addUser(userID);

################################################################################
################################################################################
################################################################################
# Parse Messages from DB and Generate Statistics

print();
print('Parsing messages...');
for row in cur.execute('SELECT * FROM group_' + groupID):
    messageID = row[0];
    messageJSON = row[1];
    messageData = json.loads(messageJSON);

    senderID = messageData['sender_id'];
    senderName = messageData['name'];
    avatarURL = messageData['avatar_url'];
    timestamp = messageData['created_at'];
    likes = messageData['favorited_by'];

    # Add User to Users if not already present
    addUserIfNotPresent(senderID)

    # Increment Message Count
    users[senderID]['messages'] = users[senderID]['messages'] + 1;

    # Increment Avatar Count
    if avatarURL not in users[senderID]['avatars']:
        users[senderID]['avatars'].append(avatarURL);

    # Increment Name Count
    if senderName not in users[senderID]['names']:
        users[senderID]['names'].append(senderName);

    # Update most recent post, as appropriate 
    if users[senderID]['lastPost'] == 0:
        users[senderID]['lastPost'] = timestamp;
    if timestamp > users[senderID]['lastPost']:
        users[senderID]['lastPost'] = timestamp;

    # Go through all attachments, incrementing counters as appropriate
    for attachment in messageData['attachments']:
        attachmentType = attachment['type'];

        if attachmentType == 'image':
            users[senderID]['attachments']['images'] = users[senderID]['attachments']['images'] + 1;
        elif attachmentType == 'linked_image':
            users[senderID]['attachments']['linked_images'] = users[senderID]['attachments']['linked_images'] + 1;
        elif attachmentType == 'video':
            users[senderID]['attachments']['videos'] = users[senderID]['attachments']['videos'] + 1;
        elif attachmentType == 'poll':
            users[senderID]['attachments']['polls'] = users[senderID]['attachments']['polls'] + 1;
        elif attachmentType == 'file':
            users[senderID]['attachments']['files'] = users[senderID]['attachments']['files'] + 1;
        elif attachmentType == 'emoji':
            users[senderID]['attachments']['emojis'] = users[senderID]['attachments']['emojis'] + 1;
        elif attachmentType == 'reply':
            users[senderID]['attachments']['replies'] = users[senderID]['attachments']['replies'] + 1;
        elif attachmentType == 'mentions':
            users[senderID]['attachments']['mentions'] = users[senderID]['attachments']['mentions'] + 1;
        elif attachmentType == 'postprocessing':
            continue
        else:
            print(attachment);

    # Parse Likes
    for likerID in likes:

        # Raw like data
        if likerID in users[senderID]['likedBy']:
            users[senderID]['likedBy'][likerID] = users[senderID]['likedBy'][likerID] + 1;
        else:
            users[senderID]['likedBy'][likerID] = 1;

        # Self-likes
        if likerID == senderID:
            users[senderID]['selfLikes'] = users[senderID]['selfLikes'] + 1;

        # Data for cross tables
        addUserIfNotPresent(likerID);
        users[likerID]['totalLikesGiven'] = users[likerID]['totalLikesGiven'] + 1;
        users[senderID]['totalLikesReceived'] = users[senderID]['totalLikesReceived'] + 1;
print('Done!');

################################################################################
################################################################################
################################################################################
# Output Base Statistics

print();
print('Outputting Data...');
print('  Outputing ' + outputDir + '/userStatistics.csv...');
with open(outputDir + '/userStatistics.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|');

    csvwriter.writerow([
        'ID',
        'Name',
        'Names',
        'Avatars',
        'Messages',
        'Images',
        'Videos',
        'Polls',
        'Replies',
        'Mentions',
        'Last Posted',
        'Self-Likes',
        'Likes Given',
        'Likes Received',
        'Likes/Post'
    ]);

    for userID in users:
        userData = users[userID];

        # Skip System Messages
        if userID == 'system':
            continue;

        csvwriter.writerow([
            userID,
            userLUT[userID],
            len(userData['names']),
            len(userData['avatars']),
            userData['messages'],
            userData['attachments']['images'],
            userData['attachments']['videos'],
            userData['attachments']['polls'],
            userData['attachments']['replies'],
            userData['attachments']['mentions'],
            datetime.fromtimestamp(userData['lastPost']),
            userData['selfLikes'],
            userData['totalLikesGiven'],
            userData['totalLikesReceived'],
            '{0:.2f}'.format(userData['totalLikesReceived']/userData['messages']),
        ])
print('  Done!');

################################################################################
################################################################################
################################################################################
# TopLikees

print();
print('  Outputing ' + outputDir + '/topLikees.csv...');
with open(outputDir + '/topLikees.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|')
    csvwriter.writerow(list(map(lambda userID : userLUT[userID], users.keys())))

    for userID in users:
        userData = users[userID];

        row = [ userLUT[userID] ]
        for likerID in users:
            if likerID in userData['likedBy']:
                row.append(userData['likedBy'][likerID]);
            else:
                row.append(0);
        csvwriter.writerow(row);
print('  Done!');

################################################################################
################################################################################
################################################################################
# TopLikeesPercent

print();
print('  Outputing ' + outputDir + '/topLikeesPercent.csv...');
with open(outputDir + '/topLikeesPercent.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|')
    csvwriter.writerow(list(map(lambda userID : userLUT[userID], users.keys())))

    for userID in users:
        userData = users[userID];

        row = [ userLUT[userID] ]
        for likerID in users:
            if likerID in userData['likedBy']:
                row.append('{0:.2f}'.format(users[userID]['likedBy'][likerID]/userData['messages']*100));
            else:
                row.append(0);
        csvwriter.writerow(row);
print('  Done!');
print('Done!');

# Clean up
print();
print('Cleaning up...');
con.commit();
con.close();
print('Done!'); 
exit();
