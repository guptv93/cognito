import json
import sys
import io

def convert_json_to_csv():
    filename = sys.argv[1]
    file = io.open(filename, "a", encoding="UTF-8")
    data = json.load(sys.stdin)
    symbol = data['symbol']['symbol']
    for message in data['messages']:
        created = message['created_at']
        body = message['body'].replace('\n'," ")
        id = message['id']
        user_name = message['user']['username']
        user_followers = message['user']['followers']
        user_official = message['user']['official']
        sentiment = None
        if message.get('entities') and message.get('entities').get('sentiment'):
            sentiment = message.get('entities').get('sentiment').get('basic')
        # links = []
        # if 'links' in message:
        #     links = []
        #     for link in message['links']:
        #         abc = {}
        #         abc['title'] = link.get('title')
        #         abc['description'] = link.get('description')
        #         links.append(abc)
        line_to_write = created + "||" + str(id) + "||" + str(symbol) + "||" + body\
		+ "||" + str(sentiment) + "||" + user_name + "||" + str(user_official) + "||" + str(user_followers) 
        file.write(line_to_write + "\n")
convert_json_to_csv()
