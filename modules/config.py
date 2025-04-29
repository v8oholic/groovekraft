import logging
import os
from modules.version import __version__
logger = logging.getLogger(__name__)

APP_NAME = 'GrooveKraft'

GROOVEKRAFT_USER_AGENT = 'groovekraft_by_v8oholic/' + __version__
GROOVEKRAFT_VERSION = __version__

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
