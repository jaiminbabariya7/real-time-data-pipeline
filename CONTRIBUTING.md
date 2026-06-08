# Contributing to Real-Time Data Pipeline

## Getting Started

1. Fork the repository and clone your fork
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Install dev dependencies: `make install-dev`

## Development Workflow

```bash
make install-dev  # install all deps
make lint         # run flake8 + black + isort
make format       # auto-format code
make test         # run pytest with coverage
```

## Code Standards

- **Formatter**: Black (line length 100)
- **Imports**: isort with Black profile
- **Type hints**: required on all public functions
- **Docstrings**: Google style

## Pull Requests

1. Ensure `make test` and `make lint` both pass
2. Write clear PR descriptions with motivation and context
3. Reference related issues
