import requests
from bs4 import BeautifulSoup
from collections.abc import Generator
from typing import Any, Mapping

# Dify OSS SDK 호환용 shim
try:
    from dify_plugin.interfaces.datasource.website import WebsiteCrawlDatasource
except ImportError:
    from dify_plugin import Datasource as WebsiteCrawlDatasource

from dify_plugin.entities.datasource import (
    WebSiteInfo,
    WebSiteInfoDetail,
    WebsiteCrawlMessage,
)

import html2text
import re


class URLToMarkdownDatasource(WebsiteCrawlDatasource):
    """Fetches the given URL and extracts readable Markdown content optimized for RAG ingestion."""

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

            # 🧹 1. 스크립트, 스타일, 노이즈 제거
            for tag in soup(["script", "style", "noscript", "iframe", "header", "footer", "nav", "form"]):
                tag.decompose()

            # 🧱 2. 헤더를 Markdown 헤더로 변환 (chunk 구조 보존)
            for header_tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
                prefix = "#" * int(header_tag.name[1])
                header_text = header_tag.get_text(strip=True)
                header_tag.insert_before(f"\n{prefix} {header_text}\n")

            # 🧩 3. 본문 추출 후 html2text 로 Markdown 변환
            converter = html2text.HTML2Text()
            converter.ignore_links = False
            converter.ignore_images = False
            converter.body_width = 0
            markdown_text = converter.handle(str(soup))

            # 🪶 4. 불필요한 빈 줄, 공백 정리
            markdown_text = re.sub(r"\n{3,}", "\n\n", markdown_text)
            markdown_text = markdown_text.strip()

            # ✂️ 5. 청킹 단위 개선: 헤더 단위로 분리 표시 (RAG-friendly)
            #    예: "# 제목1\n내용\n## 소제목\n내용" → 그대로 유지되도록 함
            #    -> 후단 임베딩에서 TokenTextSplitter가 잘 인식

            # 🧠 6. 메타정보 구성
            title = soup.title.string.strip() if soup.title and soup.title.string else "Untitled"
            excerpt = markdown_text[:400].replace("\n", " ")

            info = WebSiteInfoDetail(
                source_url=source_url,
                content=markdown_text,
                title=title,
                description=excerpt,
            )

            crawl_res.web_info_list = [info]
            crawl_res.status = "completed"
            crawl_res.completed = 1
            yield self.create_crawl_message(crawl_res)

        except Exception as e:
            crawl_res.status = "failed"
            yield self.create_crawl_message(crawl_res)
            raise ValueError(f"Failed to fetch URL: {str(e)}")
