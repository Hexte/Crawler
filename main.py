import asyncio
from crawl4ai import *
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
import json
import glob
from kafka import KafkaProducer
import os

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
        {
            "name": "coinmarketcap",
            "url": "https://coinmarketcap.com/headlines/news/"
        },
        {
            "name": "yahoo",
            "url": "https://finance.yahoo.com/topic/crypto/"
        },
        {
            "name": "bbc",
            "url": "https://www.bbc.com/news/topics/cyd7z4rvdm3t/"
        },
        {
            "name": "bloomberg",
            "url": "https://www.bloomberg.com/crypto"
        },
        {
            "name": "cryptoslate",
            "url": "https://cryptoslate.com/news/"
        }
        ]

    for i in range(scrape_targets.__len__()):
        tasks.append(scrapeForArticles(scrape_targets[i]["name"], scrape_targets[i]["url"]))
    await asyncio.gather(*tasks)
    tasks = []
    for i in range(scrape_targets.__len__()):
        tasks.append(scrapeArticles(scrape_targets[i]["name"]))
    await asyncio.gather(*tasks)

    sendScrapedToKafka("articles")

def sendScrapedToKafka(path):
    producer = KafkaProducer(
        bootstrap_servers='127.0.0.1:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for filename in os.listdir(path):
        if filename.endswith('.json'):
            filepath = os.path.join(path, filename)

            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)

                    if all(field in data for field in ['title', 'synopsys', 'content', 'link', 'time']):
                        future = producer.send(
                            "scraped_data",
                            value=data
                        )

                        try:
                            record_metadata = future.get(timeout=10)
                            print("sent")
                        except Exception as e:
                            print(f"error: {str(e)}")

            except json.JSONDecodeError:
                print(f"Skipping {filename}: Invalid JSON format")
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")


    producer.flush()
    producer.close()

async def scrapeForArticles(name, url):
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

async def scrapeArticles(name):
    async with AsyncWebCrawler() as crawler:
        with open("articleSchemas/article_schema_" + name + ".json", 'r', encoding='utf-8') as file:
            schema = json.load(file)
            print(schema)

        with open("outputs/output_" + name + ".json", 'r', encoding='utf-8') as file:
            scrapedArticleList = json.load(file)
            # print(scrapedArticleList)
        extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True, log_console=True)

        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=extraction_strategy,
        )

        urls = []
        for i in range(scrapedArticleList.__len__()):
            # print(scrapedArticleList[i]["link"])
            urls.append(scrapedArticleList[i]["link"])

        result = await crawler.arun_many(
            urls=urls,
            config=config
        )
        for article in result:
            for x in article:
                data = json.loads(x.extracted_content)
                title = data[0]["title"]
                title = ''.join(e for e in title if e.isalnum())
                savepath = 'articles/' + name + "/" + title + ".json"
                data[0]["link"] = x.url
                for i in range(scrapedArticleList.__len__()):
                    if scrapedArticleList[i]["link"] == x.url:
                        data[0]["time"] = scrapedArticleList[i]["time"]
                print(data)
                with open(savepath, 'w') as f:
                    f.write(json.dumps(data, indent=2))
        # for article in result:
        #     data = json.loads(article.extracted_content)
        #     with open('articles/_' + name + ".json", 'w') as f:
        #         f.write(json.dumps(data, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(scrapeArticles("beincrypto","https://beincrypto.com/news/", "schema_beincrypto.com.json"))
