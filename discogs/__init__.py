from . import db_discogs
from . import discogs_importer
from . import discogs_oauth_gui

import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
