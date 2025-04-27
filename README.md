# GrooveKraft

GrooveKraft is a music collection manager that integrates Discogs and MusicBrainz databases. It uses the Discogs API to
import your collection into a local SQLite database, and the MusicBrainz API to match those items. Matching gets you a lot of release
dates, and a local copy of the main artwork.

The app includes:

- 'On this day' function, to show you release anniversaries in your collection.
- Collection viewer, with filters.
- Randomizer function, to suggest something to listen to

It is meant to be a bit quicker to use than the Discogs website, with the additon of those release dates.

Some release dates will still be missing after the import and match process. Most titles will have a full release date, some
may have just a month and year, a few just a year. However the app allows you to edit the missing release dates.

Other sources of release dates include:

- Wikipedia
- 45worlds.com

The release dates which are set automatically are based on the original release date of the master release, not a re-issue date.
It should be the same regardless of which particular release you may have.

The Discogs importer is reasonably lightweight, but the API is rate-limited. On the first import, the artwork will be downloaded
at the same time, and saved locally, so it will be slower. Subsequent imports will be quicker. Import only needs to be re-run
if changes are made to your collection on Discogs.

The MusicBrainz matcher only processes items which have been updated since it was last run. The first run therefore, will be
quite slow, since everything has changed. But after that, it will be much quicker, since most items won't have changed.

The matching process has various strategies to try to find the same release in MusicBrainz, but some things just don't exist.
The success rate is still high. Fuzzy matching is used to improve matches. The match success is shown using a traffic light
icon system of Red, Amber, Yellow, Green. Red means no match. Green means a perfect match.

## Requirements

- Python 3.11+
- macOS (tested)
- Virtual environment (recommended)
- `pyinstaller` installed (`pip install pyinstaller`)
- `sips` and `iconutil` (macOS built-in utilities)

The mac-specific parts are used to build the distribution .dmg and the icon set. Most of the code should be platform-agnostic though.

## Installation

1. Clone the repository:

```bash
git clone https://github.com/v8oholic/groovekraft.git
cd groovekraft
```

2. If using conda (recommended), create an environment using
```bash
conda env create -f environment.yml
conda activate groovekraft
```

Documention is not going to be much more comprehensive than this, as I built this app for my own use 😀