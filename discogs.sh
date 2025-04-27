#!/bin/zsh

# launch script for Discogs app
# Conda environment (if appropriate) is derived from the python location
#
# To publish:
#
# ./discogs.sh --publish
#
# This will create (or update) a symlink at ~/bin/discogs pointing to this script.
#
# Make sure ~/bin is in your PATH by adding this to your ~/.zshrc if needed:
# export PATH="$HOME/bin:$PATH"
#
# After editing ~/.zshrc, reload it with:
# source ~/.zshrc
#
# (or just quit the session and start again)
#
# Now you can verify the link works with:
#
# which discogs
#
# You can invoke the app from anywhere in a shell like this:
#
# discogs
#
# arguments may be specified.

APP_PYTHON=/opt/homebrew/Caskroom/miniforge/base/envs/discogs/bin/python
APP_MAIN="$HOME/Applications/Python/Discogs/main.py"

# Determine application directory from APP_MAIN
APP_DIR=$(dirname "$APP_MAIN")

# Save current directory
OLD_DIR=$(pwd)

# Automatically restore the original directory on exit
trap 'cd "$OLD_DIR"' EXIT

# Handle self-publishing
if [[ "$1" == "--publish" ]]; then
  echo "Publishing discogs to ~/bin/discogs..."
  mkdir -p ~/bin
  SCRIPT_PATH=$(realpath "$0")
  ln -sf "$SCRIPT_PATH" ~/bin/discogs
  chmod +x "$SCRIPT_PATH"
  echo "Symlink created: ~/bin/discogs -> $SCRIPT_PATH"
  echo

  # Check if ~/bin is in PATH
  if [[ ":$PATH:" != *":$HOME/bin:"* ]]; then
    echo "Warning: ~/bin is not in your PATH."
    echo "Add this line to your ~/.zshrc:"
    echo 'export PATH="$HOME/bin:$PATH"'
    echo "Then run: source ~/.zshrc"
  fi

  exit 0
fi

# Change to application directory
cd "$APP_DIR"

# Check if the Conda environment exists
ENV_DIR=$(dirname "$APP_PYTHON")
if [ ! -d "$ENV_DIR" ]; then
  echo "Conda environment directory not found: $ENV_DIR"
  exit 1
fi

# Check if the Python executable exists
if [ ! -x "$APP_PYTHON" ]; then
  echo "Python executable not found: $APP_PYTHON"
  exit 1
fi

# Run the application, forwarding all arguments
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  echo "Usage: discogs [options] [arguments]"
  echo "Options:"
  echo "  --publish     Install or update the discogs command in ~/bin"
  echo "  -h, --help    Show this help message"
  echo
  echo "All other arguments are passed to the Python app."
  exit 0
fi

$APP_PYTHON "$APP_MAIN" "$@"
