import sqlite3, json, csv
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

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

likestamps = [];

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

    if '18823818' in likes:
        likestamps.append(timestamp);
print('Done!');

print(likestamps);

earliest = min(likestamps);
latest = max(likestamps);
time = latest-earliest

timePeriod = (24*60*60)

steps = [latest]
while steps[-1] > earliest:
    steps.append(steps[-1] - timePeriod)

result = []

i = 0;
for step in steps:
    count = 0
    for timestamp in likestamps:
        if timestamp < step and timestamp > (step - timePeriod):
            count = count + 1
    result.append(count)

print(result)

#result = result[0:365]
#result.reverse();

df = pd.DataFrame(result)
print('Mean: ' + str(df.mean()))
print('Median: ' + str(df.median()))
print('Std. Dev: ' + str(df.std()))
print(sum(result))

plt.plot(range(0,-len(result),-1), result)
plt.title('Likes Per 24 Hours - Past ' + str(len(result)) + ' Days - Eric')
plt.ylabel('Number of Likes Given Per 24 hour period')
plt.xlabel('Number of Days in Past')
plt.show()



# Clean up
print();
print('Cleaning up...');
con.commit();
con.close();
print('Done!'); 
exit();
