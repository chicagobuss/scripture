# Local non-secret config

- `templates/` — placeholder YAML (safe to keep)
- `rendered/` — gitignored output of `bin/render-config.sh PROFILE.env`
- Profile env example: `profiles/rustfs.example.env` (copy locally; fill values)

Never commit rendered YAML with real buckets/endpoints if policy forbids it;
this tree is already under ignored `config/local/`.
