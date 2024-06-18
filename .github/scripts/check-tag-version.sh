#!/bin/bash

ret=0

function check_version {
  if [[ "$1" != "$2" ]]; then
    echo "Error: The version Cargo.toml does not match the tag pushed; Tag: $1 Cargo: $2"
    ret=1
  fi
}


current_tag=${GITHUB_REF#'refs/tags/v'}

cargo_version="$(grep '^version' Cargo.toml | cut -d '=' -f2 | tr -d '"' | tr -d ' ')"

check_version "$current_tag" "$cargo_version"

if [[ "$ret" -eq 0 ]] ; then
  echo 'OK'
fi

exit $ret
