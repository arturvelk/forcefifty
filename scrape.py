import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import random


def parse_soup(self, soup):

    try:
        title = soup.find(attrs={"class": "content-title"}).get_text()
    except:
        title = ""

    try:
        text = " ".join(
            [
                item.get_text().strip()
                for item in soup.find(attrs={"class": "cikk-torzs"}).find_all("p")
            ]
        )
    except:
        text = ""
    return {
        "title": title,
        "text": text,
    }


if __name__ == "__main__":

    link = "https://index.hu/sitemap/cikkek_202203.xml"
    resp = requests.get(link)
    soup = bs(resp.content, features="xml")

    links = [item.get_text() for item in soup.find_all("loc")]

    dates = [i[i.find("2022/") : i.find("2022/") + 10] for i in links]

    url_index_1 = (
        pd.DataFrame(links, dates)
        .reset_index()
        .rename({"index": "date", 0: "url"}, axis=1)
        .sort_values(by=["date"])
    )

    random_cikkek = [random.randint(1, url_index_1.shape[0]) for x in range(10)]
    save_df = pd.DataFrame(
        {"url": ["first is nonexistent"]}
    )  # , "title": ["title"], "text": ["text"]})
    for i in random_cikkek:
        resp = requests.get(url_index_1["url"].values[i])
        soup = bs(resp.content, features="html.parser")
        save_df = pd.concat(
            [
                save_df,
                pd.DataFrame(
                    {
                        "url": [url_index_1["url"].values[i]],
                        "title": [parse_soup("a", soup)["title"]],
                        "text": [parse_soup("a", soup)["text"]],
                    }
                ),
            ]
        ).dropna()
    print_df = save_df.merge(url_index_1, on="url", how="left")
    print(print_df["url"])

    sad_df = pd.read_parquet("napi_tiz.parquet")
    print(sad_df["title"])
    # save_df.merge(url_index_1, on="url", how="left").to_parquet("napi_tiz.parquet")
