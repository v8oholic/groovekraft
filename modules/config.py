import logging
import os

logger = logging.getLogger(__name__)

APP_VERSION = '1.0'
APP_NAME = 'GrooveKraft'

GROOVEKRAFT_USER_AGENT = 'groovekraft_by_v8oholic/1.0'
GROOVEKRAFT_VERSION = '1.0'

DISCOGS_CONSUMER_KEY = 'yEJrrZEZrExGHEPjNQca'
DISCOGS_CONSUMER_SECRET = 'isFjruJTfmmXFXiaywRqCUSkIGwHlHKn'


class AppConfig:
    def __init__(self, args, root_folder):
        self.root_folder = os.getcwd()
        self.verbose = args.verbose
        self.discogs_consumer_key = DISCOGS_CONSUMER_KEY
        self.discogs_consumer_secret = DISCOGS_CONSUMER_SECRET
        self.user_agent = GROOVEKRAFT_USER_AGENT
        self.app_version = GROOVEKRAFT_VERSION
