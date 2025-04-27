import logging

logger = logging.getLogger(__name__)

APP_VERSION = '1.0'

GROOVEKRAFT_USER_AGENT = 'groovekraft_by_v8oholic/1.0'
GROOVEKRAFT_VERSION = '1.0'

DISCOGS_CONSUMER_KEY = 'yEJrrZEZrExGHEPjNQca'
DISCOGS_CONSUMER_SECRET = 'isFjruJTfmmXFXiaywRqCUSkIGwHlHKn'


class AppConfig:
    def __init__(self, args, root_folder):

        self.verbose = args.verbose
        self.consumer_key = None
        self.consumer_secret = None
        self.oauth_token = None
        self.oauth_token_secret = None
        self.username = None
        self.password = None
        self.root_folder = root_folder
        self.app_version = APP_VERSION

    def load_from_config_parser(self, config_parser):
        self.consumer_key = config_parser.get('Discogs', 'consumer_key', fallback=None)
        self.consumer_secret = config_parser.get('Discogs', 'consumer_secret', fallback=None)
        self.oauth_token = config_parser.get('Discogs', 'oauth_token', fallback=None)
        self.oauth_token_secret = config_parser.get('Discogs', 'oauth_token_secret', fallback=None)
        self.username = config_parser.get('MusicBrainz', 'username', fallback=None)
        self.password = config_parser.get('MusicBrainz', 'password', fallback=None)
