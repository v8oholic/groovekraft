#!/bin/zsh

# launch script for GrooveKraft app
# Conda environment (if appropriate) is derived from the python location
#
# To publish:
#
# ./groovekraft.sh --publish
#
# This will create (or update) a symlink at ~/bin/groovekraft pointing to this script,
# and install the app bundle into ~/Applications so Spotlight can find it.
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
# which groovekraft
#
# You can invoke the app from anywhere in a shell like this:
#
# groovekraft
#
# arguments may be specified.

APP_PYTHON=/opt/homebrew/Caskroom/miniforge/base/envs/groovekraft/bin/python
APP_MAIN="$HOME/Applications/Python/GrooveKraft/groovekraft.py"

# Determine application directory from APP_MAIN
APP_DIR=$(dirname "$APP_MAIN")

# Save current directory
OLD_DIR=$(pwd)

# Automatically restore the original directory on exit
trap 'cd "$OLD_DIR"' EXIT

# Handle self-publishing
if [[ "$1" == "--publish" ]]; then
  echo "Publishing groovekraft to ~/bin/groovekraft..."
  mkdir -p ~/bin
  SCRIPT_PATH=$(realpath "$0")
  chmod +x "$SCRIPT_PATH"
  ln -sf "$SCRIPT_PATH" ~/bin/groovekraft
  echo "Symlink created: ~/bin/groovekraft -> $SCRIPT_PATH"
  echo

  # Install the app bundle so it is available as an application (Spotlight indexed)
  APP_BUNDLE_SRC="$APP_DIR/dist/GrooveKraft.app"
  APP_BUNDLE_DEST="$HOME/Applications/GrooveKraft.app"
  if [ -d "$APP_BUNDLE_SRC" ]; then
    echo "Installing app bundle to ~/Applications..."
    mkdir -p "$HOME/Applications"
    rm -rf "$APP_BUNDLE_DEST"
    cp -R "$APP_BUNDLE_SRC" "$APP_BUNDLE_DEST"
    echo "App installed: $APP_BUNDLE_DEST"
  else
    echo "Warning: App bundle not found at $APP_BUNDLE_SRC"
    echo "Run ./make_installer.sh first to build GrooveKraft.app"
  fi

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
  echo "Usage: groovekraft [options] [arguments]"
  echo "Options:"
  echo "  --publish     Install or update the groovekraft command in ~/bin"
  echo "  -h, --help    Show this help message"
  echo
  echo "All other arguments are passed to the Python app."
  exit 0
fi

$APP_PYTHON "$APP_MAIN" "$@"
