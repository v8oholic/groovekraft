from configparser import ConfigParser, ExtendedInterpolation
import os


def get_app_config(config_filename='discogs.ini'):

    if os.path.isfile(config_filename):
        config = ConfigParser(allow_no_value=True,
                              interpolation=ExtendedInterpolation(), default_section="Common")
        config.read(config_filename)

    else:

        config = ConfigParser(allow_no_value=True,
                              interpolation=ExtendedInterpolation(), default_section="Common")
        config.add_section("Common")
        config["Common"]["database_name"] = 'local'
        config["Common"]["user_agent"] = 'v8oholic_collection_application/1.0'

        config.add_section("Discogs")
        config["Discogs"]["consumer_key"] = ''
        config["Discogs"]["consumer_secret"] = ''

        config.add_section("MusicBrainz")
        config["MusicBrainz"]["username"] = ''
        config["MusicBrainz"]["password"] = ''

        with open(config_filename, "w") as file_object:
            config.write(file_object)

    return config
