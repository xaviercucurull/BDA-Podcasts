"""
CONFIGURATION FILE
"""

SPOTIPY_CLIENT_ID ='8c8189fe204c401fab8b265ce857a834'       # Your spotify Client ID
SPOTIPY_CLIENT_SECRET ='4f31f73ce5934fafa4914d9a4b3d37bb'   # Your spotify Client Secret

PODCASTS_FILE = 'data/apple_podcasts.csv'

MAX_SHOWS_PER_SEARCH = 5
BATCH_SIZE = 1000
OFFSET = 0                          # Batches to skip from processing
POOL_PROCESSES = 5

DATABASE_NAME = 'finalproject'      # Name of the MongoDB database
COLLECTION_NAME = 'podcasts'        # Name of the MongoDB collection
