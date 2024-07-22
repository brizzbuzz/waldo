_default:
  just --list

setup:
  op inject -i waldo.tpl.yml -o waldo.yml -f

build:
  cargo build

run:
  cargo run

nbuild:
  nix build

nrun:
  nix run

ncheck:
  nix flake check
