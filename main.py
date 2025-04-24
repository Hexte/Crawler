import asyncio
from crawl4ai import *
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
import json
import glob

async def main():
    tasks = []
    scrape_targets = [
        {
            "name": "cryptonews",
            "url": "https://cryptonews.com/news/"
        },
        {
            "name": "coindesk",
            "url": "https://www.coindesk.com/latest-crypto-news/"
        },
        {
            "name": "beincrypto",
            "url": "https://beincrypto.com/news/"
        },
        ]

    scrape_targets_dump = json.dumps(scrape_targets)
    print()
    # print(scrape_targets)
    for i in range(scrape_targets.__len__()):
        tasks.append(scrapeArticles(scrape_targets[i]["name"],scrape_targets[i]["url"]))
    await asyncio.gather(*tasks)


async def scrapeArticles(name, url):
    async with AsyncWebCrawler() as crawler:
        with open("mainSchemas/schema_" + name + ".json", 'r', encoding='utf-8') as file:
            schema = json.load(file)
            print(schema)
        extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)

        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=extraction_strategy,
        )


        result = await crawler.arun(
            url=url,
            config=config
        )

        data = json.loads(result.extracted_content)
        with open('outputs/output_' + name + ".json", 'w') as f:
            f.write(json.dumps(data, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(scrapeArticles("beincrypto","https://beincrypto.com/news/", "schema_beincrypto.com.json"))
