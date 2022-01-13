#!/bin/bash

set -e

function get_major_version() {
  echo "$1" | cut -d '.' -f 1
}

function get_minor_version() {
  echo "$1" | cut -d '.' -f 2
}

function trim_version() {
  grep -e "$1" "$2" | cut -d "=" -f 2 | tr -d "\"^ "
}

function check_versions() {
  if [ "$1" != "$2" ]; then
    echo "aleph-bft Cargo's toml $3 version $1 different than README.md's $3 version $2!"
    exit 1
  fi
}

cargo_toml_version=$(trim_version '^version =' "Cargo.toml")
cargo_toml_major_version=$(get_major_version "${cargo_toml_version}")
cargo_toml_minor_version=$(get_minor_version "${cargo_toml_version}")

readme_version=$(trim_version '\s*aleph-bft =' "README.md")
readme_major_version=$(get_major_version "${readme_version}")
readme_minor_version=$(get_minor_version "${readme_version}")

check_versions "${cargo_toml_major_version}" "${readme_major_version}" "major"
check_versions "${cargo_toml_minor_version}" "${readme_minor_version}" "minor"
echo "Versions from README and Cargo.toml match."
