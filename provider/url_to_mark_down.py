from typing import Any, Mapping
from dify_plugin.interfaces.datasource import DatasourceProvider


class URLToMarkdownProvider(DatasourceProvider):
    """Credential validation skipped (for local/experimental use)."""

    def _validate_credentials(self, credentials: Mapping[str, Any]) -> None:
        # テスト用であるため認証をスキップ
        return True
