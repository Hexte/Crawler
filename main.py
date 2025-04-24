import asyncio
from crawl4ai import *
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
import json

async def main():
    async with AsyncWebCrawler() as crawler:
        # schema = {
        #             "name": "cryptonews article",
        #             "baseSelector": ".archive-template-latest-news__wrap",
        #             "fields": [
        #             {
        #                 "name": "title",
        #                 "selector": ".archive-template-latest-news__title",
        #                 "type": "text"
        #             },
        #             {
        #                 "name": "link",
        #                 "selector": ".archive-template-latest-news",
        #                 "type": "attribute",
        #                 "attribute": "href"
        #             },
        #             {
        #                 "name": "time",
        #                 "selector": ".archive-template-latest-news__time",
        #                 "type": "attribute",
        #                 "attribute": "data-utctime"
        #             }
        #             ]
        #         }
        with open('schema_cryptonews.com.json', 'r', encoding='utf-8') as file:
            schema = json.load(file)
            print(schema)
        extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)

        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=extraction_strategy,
        )

        result = await crawler.arun(
            url="https://cryptonews.com/news/",
            config=config
        )

        data = json.loads(result.extracted_content)
        print(f"Extracted {len(data)} entries")
        print(json.dumps(data, indent=2) if data else "No data found")

if __name__ == "__main__":
    asyncio.run(main())