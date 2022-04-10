import logging
import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

import click
import pandas as pd


MOVIELENS_FETCH_URL = "http://files.grouplens.org/datasets/movielens/ml-25m.zip"


logging.basicConfig(
    format="[%(asctime)-15s] %(levelname)s - %(message)s", level=logging.INFO
)


def fetch_ratings(url):
    """
    Fetches ratings from the given URL

    Parameters
    ----------
    url: str
        url of fetch object

    :return:
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir, "download.zip")
        logging.info(f"Downloading zip file from {url}")
        urlretrieve(url, tmp_path)

        with zipfile.ZipFile(tmp_path) as zip_:
            logging.info(f"Downloaded zip file with contents: {zip_.namelist()}")
            logging.info("Reading ml-25m/ratings.csv from zip file")
            with zip_.open("ml-25m/ratings.csv") as file_:
                ratings = pd.read_csv(file_)

    return ratings


@click.command()
@click.option("--start_date", default="2019-01-01", type=click.DateTime())
@click.option("--end_date", default="2020-01-01", type=click.DateTime())
@click.option("--output_path", required=True)
def main(start_date, end_date, output_path):
    """
    Script for fetching movielens ratings within a given date range.
    :param start_date:
    :param end_date:
    :param output_path:
    :return:
    """
    logging.info(f"Filtering for dates {start_date} - {end_date} ...")
    ratings = fetch_ratings(MOVIELENS_FETCH_URL)
    timestamp_parsed = pd.to_datetime(ratings["timestamp"], unit="s")
    ratings = ratings.loc[(timestamp_parsed >= start_date) & (timestamp_parsed < end_date)]
    logging.info(f"Writing ratings to '{output_path}'...")
    ratings.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()