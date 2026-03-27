import logging
import os
from shared.version import __version__
logger = logging.getLogger(__name__)

APP_NAME = 'GrooveKraft'

GROOVEKRAFT_USER_AGENT = 'groovekraft_by_v8oholic/' + __version__
GROOVEKRAFT_VERSION = __version__

DISCOGS_CONSUMER_KEY = 'yEJrrZEZrExGHEPjNQca'
DISCOGS_CONSUMER_SECRET = 'isFjruJTfmmXFXiaywRqCUSkIGwHlHKn'


class AppConfig:
    def __init__(self, args, root_folder):
        self.root_folder = root_folder or os.getcwd()
        self.verbose = getattr(args, "verbose", False)
        self.server_mode = getattr(args, "server", False)
        self.server_host = getattr(args, "host", "127.0.0.1")
        self.server_port = getattr(args, "port", 8000)
        self.mb_username = ""
        self.mb_password = ""
        self.discogs_consumer_key = DISCOGS_CONSUMER_KEY
        self.discogs_consumer_secret = DISCOGS_CONSUMER_SECRET
        self.app_name = APP_NAME
        self.user_agent = GROOVEKRAFT_USER_AGENT
        self.app_version = GROOVEKRAFT_VERSION
