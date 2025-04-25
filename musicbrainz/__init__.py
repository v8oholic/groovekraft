from . import mb_auth_gui
from . import mb_matcher
from . import db_musicbrainz

import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
