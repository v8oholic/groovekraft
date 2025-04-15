# modules/__init__.py

from . import config
from . import db
from . import utils
from . import discogs_importer
from . import mb_matcher

import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
