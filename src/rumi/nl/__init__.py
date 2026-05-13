"""NL-to-SQL pipeline.

Turns natural-language prompts into governed SQL. Consumes
governance.preview_schema() to construct prompts that reference only
allowed columns and metrics. Generates candidate alternatives on denial
and re-validates via governance.validate_alternatives() (governance never
generates -- only validates)."""
