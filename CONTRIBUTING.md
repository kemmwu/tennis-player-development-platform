# Contributing

This is a personal portfolio project. Issues and feedback are welcome.

## Development Setup

1. **Clone the repo**
```bash
   git clone https://github.com/kemmwu/tennis-player-development-platform.git
   cd tennis-player-development-platform
```

2. **Create virtual environment**
```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
```

3. **Configure dbt**
```bash
   cp tennis_analytics/profiles_example.yml ~/.dbt/profiles.yml
   # Edit with your Databricks credentials
```

4. **Install pre-commit hooks**
```bash
   pip install pre-commit
   pre-commit install
```

5. **Run dbt**
```bash
   cd tennis_analytics
   dbt deps
   dbt build
```

## Branch Naming

- `feature/description` — new features
- `fix/description` — bug fixes
- `day{N}-description` — project day work

## Pull Requests

- All changes go through PRs
- CI must pass before merge
- Branch protection on `main`
