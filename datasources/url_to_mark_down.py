import requests
from bs4 import BeautifulSoup
from collections.abc import Generator
from typing import Any, Mapping

from dify_plugin.entities.datasource import (
    WebSiteInfo,
    WebSiteInfoDetail,
    WebsiteCrawlMessage,
)
from dify_plugin.interfaces.datasource.website import WebsiteCrawlDatasource


class URLToMarkdownDatasource(WebsiteCrawlDatasource):
    """Fetches the given URL and extracts readable text from it."""

    def _get_website_crawl(
        self, datasource_parameters: Mapping[str, Any]
    ) -> Generator[WebsiteCrawlMessage, None, None]:
        source_url = datasource_parameters.get("url")
        if not source_url:
            raise ValueError("url is required")

        crawl_res = WebSiteInfo(web_info_list=[], status="processing", total=1, completed=0)
        yield self.create_crawl_message(crawl_res)

        try:
            resp = requests.get(source_url, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            text = "\n".join(line.strip() for line in soup.get_text().splitlines() if line.strip())

            info = WebSiteInfoDetail(
                source_url=source_url,
                content=text,
                title=soup.title.string if soup.title else "",
                description=text[:200],
            )

            crawl_res.web_info_list = [info]
            crawl_res.status = "completed"
            crawl_res.completed = 1
            yield self.create_crawl_message(crawl_res)

        except Exception as e:
            crawl_res.status = "failed"
            yield self.create_crawl_message(crawl_res)
            raise ValueError(f"Failed to fetch URL: {str(e)}")
