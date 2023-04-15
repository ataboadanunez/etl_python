import json
from IPython import embed
f = open('settings.json')
config = json.load(f)
CLIENT_ID = config['CLIENT_ID']
CLIENT_SECRET = config['CLIENT_SECRET']
SPOTIPY_REDIRECT_URI = config['SPOTIPY_REDIRECT_URI']
f.close()