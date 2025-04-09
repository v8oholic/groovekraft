from configparser import ConfigParser, ExtendedInterpolation
import os


class AppConfig:
    def __init__(self, args):
        self.dry_run = args.dry_run
        self.all_items = args.all_items
        self.new_items = args.new_items
        self.reset = args.reset
        self.match = args.match
        self.release_date = args.release_date
        self.discogs_id = args.discogs_id
        self.mbid = args.mbid

        self.database_name = args.database or "local.db"
        self.user_agent = 'v8oholic_collection_application/1.0'
        self.consumer_key = None
        self.consumer_secret = None
        self.oauth_token = None
        self.oauth_token_secret = None
        self.username = None
        self.password = None

    def load_from_config_parser(self, config_parser):
        self.consumer_key = config_parser.get('Discogs', 'consumer_key', fallback=None)
        self.consumer_secret = config_parser.get('Discogs', 'consumer_secret', fallback=None)
        self.oauth_token = config_parser.get('Discogs', 'oauth_token', fallback=None)
        self.oauth_token_secret = config_parser.get('Discogs', 'oauth_token_secret', fallback=None)
        self.username = config_parser.get('MusicBrainz', 'username', fallback=None)
        self.password = config_parser.get('MusicBrainz', 'password', fallback=None)
        self.user_agent = config_parser.get('Common', 'user_agent', fallback=self.user_agent)
        self.database_name = config_parser.get(
            'Common', 'database_name', fallback=self.database_name)


# def get_app_config(config_filename='discogs.ini'):

#     if os.path.isfile(config_filename):
#         config = ConfigParser(allow_no_value=True,
#                               interpolation=ExtendedInterpolation(), default_section="Common")
#         config.read(config_filename)

#     else:

#         config = ConfigParser(allow_no_value=True,
#                               interpolation=ExtendedInterpolation(), default_section="Common")
#         config.add_section("Common")
#         config["Common"]["database_name"] = 'local'
#         config["Common"]["user_agent"] = 'v8oholic_collection_application/1.0'

#         config.add_section("Discogs")
#         config["Discogs"]["consumer_key"] = ''
#         config["Discogs"]["consumer_secret"] = ''

#         config.add_section("MusicBrainz")
#         config["MusicBrainz"]["username"] = ''
#         config["MusicBrainz"]["password"] = ''

#         with open(config_filename, "w") as file_object:
#             config.write(file_object)

#     return config
