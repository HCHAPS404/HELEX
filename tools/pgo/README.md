# PGO training data

`generate` is the challenge's dataset generator (Linux x86-64 build, taken
from the team's `opt/post-alex` branch, `vmbin/generate`). The Docker build
runs it to produce a small official-shaped dataset (seed 7, 2M events) and
profiles the engine on it for profile-guided optimization. Any failure in the
PGO steps falls back to a plain release build.
