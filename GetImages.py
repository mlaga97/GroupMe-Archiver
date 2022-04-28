import sqlite3, json, os
from pathlib import Path

# Load Configuration
print('Loading configuration...');
with open('config.json') as f:
    config = json.load(f);
    groupID = config['groupID'];
    dbPath = config['dbPath'];
    avatarCache = config['avatarCache'];
    attachmentCache = config['attachmentCache'];
print('Done!');

# Open Database
print();
print('Connecting to DB...');
con = sqlite3.connect(dbPath);
cur = con.cursor();
print('Done!');

# Globals
avatar_urls = [];
attachment_urls = [];

# Get List of Existing Messages
print();
print('Retrieving and parsing cached messages from DB...');
for row in cur.execute('SELECT * FROM group_' + groupID):
    messageID = row[0];
    messageJSON = row[1];
    messageData = json.loads(messageJSON);

    #print(json.dumps(messageData, indent = 2));

    # Avatar URLs
    avatarURL = str(messageData['avatar_url']);
    if avatarURL not in avatar_urls:
        avatar_urls.append(avatarURL);
        #print('New avatar_url "' + avatarURL + '" found!');

    # Attachments
    for attachment in messageData['attachments']:
        if attachment['type'] == 'image':
            attachmentURL = attachment['url'];
            if attachmentURL not in attachment_urls:
                attachment_urls.append(attachmentURL);
        elif attachment['type'] == 'linked_image':
            attachmentURL = attachment['url'];
            if attachmentURL not in attachment_urls:
                attachment_urls.append(attachmentURL);
        elif attachment['type'] == 'video':
            attachmentURL = attachment['url'];
            if attachmentURL not in attachment_urls:
                attachment_urls.append(attachmentURL);
        elif attachment['type'] == 'poll':
            a = 1;
        elif attachment['type'] == 'file':
            # TODO
            a = 1;
        elif attachment['type'] == 'emoji':
            a = 1;
        elif attachment['type'] == 'reply':
            a = 1;
        elif attachment['type'] == 'mentions':
            a = 1;
        elif attachment['type'] == 'postprocessing':
            # TODO
            a = 1;
        else:
            print(attachment);
print('Done!');

print();
print('Checking local image cache...');
for avatar_url in avatar_urls:
    components = avatar_url.replace('https://', '').replace('com/', '').split('.');
    if len(components) == 1:
        continue

    extension = components[3];
    name = components[4];

    outputPath = avatarCache + '/' + name + '.' + extension;

    if not Path(outputPath).exists():
        print("Need " + outputPath);
        os.system('curl "' + avatar_url + '" --output ' + outputPath);

for attachment_url in attachment_urls:
    components = attachment_url.replace('https://', '').replace('com/', '').split('.');
    if len(components) == 1:
        continue

    if components[0] == 'i':
        extension = components[3];
        name = components[4];

        outputPath = attachmentCache + '/i/' + name + '.' + extension;

        if not Path(outputPath).exists():
            print("Need " + outputPath);
            os.system('curl "' + attachment_url + '" --output ' + outputPath);

    if components[0] == 'v':
        subcomponents = components[2].split('/');
        extension = components[4];
        name = subcomponents[2];

        outputPath = attachmentCache + '/v/' + name + '.' + extension;

        if not Path(outputPath).exists():
            print("Need " + outputPath);
            os.system('curl "' + attachment_url + '" --output ' + outputPath);
print('Done!');

# Clean up
print();
print('Cleaning up...');
con.commit();
con.close();
print('Done!'); 
exit();
