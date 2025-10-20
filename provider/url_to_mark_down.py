from typing import Any, Mapping
from dify_plugin.interfaces.datasource import DatasourceProvider


class URLToMarkdownProvider(DatasourceProvider):
    """Credential validation skipped (for local/experimental use)."""

    def _validate_credentials(self, credentials: Mapping[str, Any]) -> None:
        # 실험용이라 검증 없이 항상 통과
        return True
