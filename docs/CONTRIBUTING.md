# Contributing to DataLakeHouse

We welcome contributions! Please follow these guidelines to ensure code quality and consistency.

## Code Style

- **Python**: Follow PEP 8 guidelines. Use `snake_case` for functions and variables, `CamelCase` for classes.
- **SQL**: Use `UPPERCASE` for SQL keywords (SELECT, FROM, WHERE).
- **Comments**: Add docstrings to all functions and classes. Use inline comments for complex logic. Use English for all documentation.

## Workflow

1.  **Fork the repository**.
2.  **Create a branch** for your feature or fix (`git checkout -b feature/my-feature`).
3.  **Commit your changes** with clear messages.
4.  **Push to the branch** (`git push origin feature/my-feature`).
5.  **Open a Pull Request**.

## Testing

- Verify that `docker compose up` runs without errors.
- Ensure the ETL pipeline executes successfully in Dagster.
- Test the Streamlit app to ensure no regressions.

## Authors
**Nguyễn Đức Linh & Vũ Trung Kiên**
