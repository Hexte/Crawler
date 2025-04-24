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
            "url": "https://cryptonews.com/news/",
            "schemaName": "schema_cryptonews.com.json"
        },
        {
            "name": "coindesk",
            "url": "https://www.coindesk.com/latest-crypto-news/",
            "schemaName": "schema_coindesk.com.json"
        },
        {
            "name": "beincrypto",
            "url": "https://beincrypto.com/news/",
            "schemaName": "schema_beincrypto.com.json"
        },
        ]

    scrape_targets_dump = json.dumps(scrape_targets)
    print()
    # print(scrape_targets)
    for i in range(scrape_targets.__len__()):
        tasks.append(scrapeArticles(scrape_targets[i]["name"],scrape_targets[i]["url"], scrape_targets[i]["schemaName"]))
    await asyncio.gather(*tasks)


async def scrapeArticles(name, url, schemaName):
    async with AsyncWebCrawler() as crawler:
        with open(schemaName, 'r', encoding='utf-8') as file:
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
        # print(f"Extracted {len(data)} entries")
        # print(json.dumps(data, indent=2))
        with open('outputs/output_' + name, 'w') as f:
            f.write(json.dumps(data, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(scrapeArticles("beincrypto","https://beincrypto.com/news/", "schema_beincrypto.com.json"))
