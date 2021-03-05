#!/bin/bash
set -ex

COMMAND="$@"

if [[ -n "$PRE_COMMAND_B64" ]]; then
  PRE_COMMAND=$(base64 --decode <<< "$PRE_COMMAND_B64")
else
  PRE_COMMAND="echo 'no pre-command defined'"
fi

if [[ -n "$POST_COMMAND_B64" ]]; then
  POST_COMMAND=$(base64 --decode <<< "$POST_COMMAND_B64")
else
  POST_COMMAND="echo 'no post-command defined'"
fi

cat > run.sh <<- EOM
set -ex

$PRE_COMMAND

$COMMAND

$POST_COMMAND

echo "Completed all commands."

EOM

chmod +x run.sh
exec "$(pwd)/run.sh"
