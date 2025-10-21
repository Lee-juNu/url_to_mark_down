import requests
from bs4 import BeautifulSoup
from collections.abc import Generator
from typing import Any, Mapping
from dify_plugin.interfaces.datasource.website import WebsiteCrawlDatasource
from dify_plugin.entities.datasource import (
    WebSiteInfo,
    WebSiteInfoDetail,
    WebsiteCrawlMessage,
)

import html2text
import re


class URLToMarkdownDatasource(WebsiteCrawlDatasource):
    """指定された URL を取得し、RAG 向けに最適化した Markdown コンテンツを抽出するデータソース"""

    def _get_website_crawl(
        self, datasource_parameters: Mapping[str, Any]
    ) -> Generator[WebsiteCrawlMessage, None, None]:
        source_url = datasource_parameters.get("url")
        if not source_url:
            raise ValueError("url is required")

        # 初期ステータス送信（処理中）
        crawl_res = WebSiteInfo(web_info_list=[], status="processing", total=1, completed=0)
        yield self.create_crawl_message(crawl_res)

        try:
            # 1. HTTP 取得（タイムアウト・文字コード補正）
            resp = requests.get(source_url, timeout=15)
            resp.encoding = resp.apparent_encoding or "utf-8"
            resp.raise_for_status()

            # 2. BeautifulSoup でパース（汎用的な html.parser 使用）
            soup = BeautifulSoup(resp.text, "html.parser")

            # 3. ノイズ要素の削除
            #    - script/style/noscript: 実行コードや非表示部
            #    - iframe: 外部埋め込み（本文と無関係なことが多い）
            #    - header/footer/nav/form: ナビゲーションや入力フォーム
            for tag in soup(["script", "style", "noscript", "iframe", "header", "footer", "nav", "form"]):
                tag.decompose()

            # 4. 見出しタグ (h1〜h5) を Markdown の見出し文字列に変換して事前挿入
            #    - 元タグ自体は残すが、前に "# 見出し" 文字列を追加し html2text の変換精度を高める
            #    - 見出しごとのチャンク分割を後段 TokenTextSplitter でしやすくする
            for header_tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
                prefix = "#" * int(header_tag.name[1])
                header_text = header_tag.get_text(strip=True)
                header_tag.insert_before(f"\n{prefix} {header_text}\n")

            # 5. html2text を用いて Markdown へ変換
            #    - リンクと画像は保持（RAG でコンテキスト価値がある）
            #    - body_width=0 で折り返し抑制し後段分割時の制御性向上
            converter = html2text.HTML2Text()
            converter.ignore_links = False
            converter.ignore_images = False
            converter.body_width = 0
            markdown_text = converter.handle(str(soup))

            # 6. 余分な空行を整理（3連以上→2連）
            #    - 空行過多はトークン効率を悪化させるため抑制
            markdown_text = re.sub(r"\n{3,}", "\n\n", markdown_text)
            markdown_text = markdown_text.strip()

            # 7. チャンク構造最適化
            #    - ここでは加工せず見出しベースの自然分割を後段に委任
            #    - 必要ならヘッダ間分割ロジックを追加可能

            # 8. メタ情報抽出
            #    - title: <title> が無ければ Untitled
            #    - excerpt: 先頭 400 文字（改行はスペース化）
            title = soup.title.string.strip() if soup.title and soup.title.string else "Untitled"
            excerpt = markdown_text[:400].replace("\n", " ")

            # 9. WebSiteInfoDetail を構築し完了メッセージ送信
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
            # 失敗時ステータス送信後に例外再送出
            crawl_res.status = "failed"
            yield self.create_crawl_message(crawl_res)
            raise ValueError(f"Failed to fetch URL: {str(e)}")
