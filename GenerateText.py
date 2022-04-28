import sqlite3, json
from datetime import datetime

# Load Configuration
with open('config.json') as f:
    config = json.load(f);
    groupID = config['groupID'];
    userLUT = config['users'];
    dbPath = config['dbPath'];

# Open Database
con = sqlite3.connect(dbPath);
cur = con.cursor();

# Get List of Existing Messages
#print();
#print('Retrieving list of cached messages from DB...');
for row in cur.execute('SELECT * FROM group_' + groupID):
    messageID = row[0];
    messageJSON = row[1];
    messageData = json.loads(messageJSON);

    #print(messageData);
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

con.close();
