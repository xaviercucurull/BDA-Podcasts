"""
CONFIGURATION FILE
"""

SPOTIPY_CLIENT_ID ='XXXXXXXXXXXXXXXXXXX'       # Your spotify Client ID
SPOTIPY_CLIENT_SECRET ='XXXXXXXXXXXXXXXXXXX'   # Your spotify Client Secret

PODCASTS_FILE = 'data/apple_podcasts.csv'

MAX_SHOWS_PER_SEARCH = 5
BATCH_SIZE = 1000
OFFSET = 0                          # Batches to skip from processing
POOL_PROCESSES = 5

DATABASE_NAME = 'finalproject'      # Name of the MongoDB database
COLLECTION_NAME = 'podcasts'        # Name of the MongoDB collection
