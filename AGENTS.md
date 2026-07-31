# AGENTS.md

## Project

- This repository implements Sampling Space-Saving Sets in Go.
- The module path is `github.com/sawmills/go-ssss`.
- Use Go 1.18 or later.
- The sketch accepts streaming label and item pairs.
- It uses fixed memory and supports sketch merges.
- Cardinality sketches use HyperLogLog.
- The implementation uses Go generics.
- The project uses the Apache License, Version 2.0.

## Map

- `ssss.go` contains the main sketch implementation.
- `config.go` contains configuration code.
- `hyperloglog.go` contains HyperLogLog code.
- `sketch_traits.go` contains sketch traits.
- `cached.go` contains cached code.
- `ssss_test.go` contains tests.
- `example/main.go` contains the usage example.
- `README.md` documents the algorithm and usage.
- `CHANGELOG.md` records project changes.

## Commands

- Run all tests: `go test -v ./...`

## Rules

- Keep the module path `github.com/sawmills/go-ssss`.
- Preserve Go 1.18 compatibility.
- Preserve generic APIs.
- Keep memory usage fixed for the sketch.
- Preserve sketch merge support.
- Keep HyperLogLog as the cardinality sketch.
- Update `README.md` when usage changes.
- Update `CHANGELOG.md` when project changes require a changelog entry.
