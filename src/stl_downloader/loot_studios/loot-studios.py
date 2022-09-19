import logging
import os
import shutil
import ssl
import subprocess
import urllib.request as urlrequest
from multiprocessing import Pool, cpu_count
from pathlib import Path
from urllib.error import URLError

from datetime import datetime
import dotenv
import requests
from chromedriver_py import binary_path
from retry import retry
from selenium import webdriver
from selenium.common import NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, subqueryload
from sqlalchemy.sql.functions import now

import stl_downloader.dropbox_uploader as dropbox
from stl_downloader import DOWNLOADS
from stl_downloader.database import Base, Collection, File, engine, initializer

# noinspection PyUnresolvedReferences,PyProtectedMember
ssl._create_default_https_context = ssl._create_unverified_context
dotenv.load_dotenv()

today = datetime.today()


def update_chromedriver():
    main_chrome_version = (
        subprocess.check_output(
            [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "--version",
            ],
            encoding="UTF-8",
        )
        .strip()
        .split()[-1]
        .split(".")[0]
    )
    subprocess.run(
        [
            "pip",
            "install",
            f"chromedriver-py>={main_chrome_version},<{main_chrome_version + 1}",
        ],
        check=True,
    )


def download(url: str, filepath: Path, collection: str):
    filename = filepath.name
    print(f"Downloading {filename} from {collection}")
    try:
        with urlrequest.urlopen(url) as response, open(
            filepath, "wb"
        ) as out_file:
            shutil.copyfileobj(response, out_file)
    except URLError as err:
        print(err, "-", filename)
    else:
        with Session(engine) as session:
            db_file = (
                session.query(File)
                .filter(
                    (File.name == filename)
                    & (File.collection_name == collection)
                )
                .one()
            )
            db_file.downloaded = True
            session.commit()
        print(f"{filename} downloaded")


def download_all(engine):
    with Session(engine) as session:
        to_download = session.query(File).filter(File.downloaded.is_(False)).all()

    try:
        with Pool(cpu_count(), initializer) as pool:
            pool.starmap(
                download,
                ((f.url, Path(f.path), f.collection_name) for f in to_download),
            )
    finally:
        with Session(engine) as session:
            # Check if all files from a collection have finished downloading
            # and set the collection to be skipped in the future if true
            collections = session.query(Collection).all()
            for collection in collections:
                finished = (
                    session.query(File)
                    .filter(
                        (
                            File.downloaded.is_(True)
                            & (File.collection_name == collection.name)
                        )
                    )
                    .count()
                )
                files_from_collection = (
                    session.query(File)
                    .where(File.collection_name == collection.name)
                    .count()
                )
                if (
                    not finished == files_from_collection == 0
                    and finished == files_from_collection
                ):
                    collection.skip = True
                    session.commit()


class LootStudios:
    logger = logging.getLogger("Loot Studios")

    def __init__(self, engine):
        self.engine = engine

    @staticmethod
    @retry(WebDriverException, tries=5, delay=1)
    def driver_get(driver, url):
        driver.get(url)

    def get_data(self):
        with Session(engine) as session:
            to_skip = {
                skip.url
                for skip in session.query(Collection)
                .where(Collection.skip.is_(True))
                .all()
            }

        with webdriver.Chrome(service=Service(binary_path)) as driver:
            self.driver_get(driver, "https://www.loot-studios.com/login")
            driver.find_element(value="member_email").send_keys(os.environ["EMAIL"])
            driver.find_element(value="member_password").send_keys(
                os.environ["LOOT_PASSWORD"].strip()
            )
            driver.find_element(By.NAME, "commit").click()

            i = 0
            while True:
                i += 1

                self.driver_get(
                    driver, f"https://www.loot-studios.com/library?page={i}"
                )
                collections = {
                    l.get_attribute("href")
                    for l in driver.find_element(
                        By.XPATH, "/html/body/div[1]/div/div[3]/div"
                    ).find_elements(By.TAG_NAME, "a")
                }

                # Stop on empty pages
                if "products" not in ",".join(collections):
                    break

                for collection in collections:
                    if "library" in collection or collection in to_skip:
                        continue

                    with Session(self.engine) as session:
                        db_collection = (
                            session.query(Collection)
                            .where(Collection.url == collection)
                            .one_or_none()
                        )
                        if db_collection is None:
                            db_collection = Collection(url=collection)
                            session.add(db_collection)
                            session.commit()

                        self.driver_get(driver, collection)
                        mainfolder = driver.find_element(
                            By.XPATH,
                            "/html/body/div[1]/div/div/div[3]/div/div/div/div/div/div/h1",
                        ).text

                        db_collection.name = mainfolder
                        session.commit()

                        collection_files = {
                            f.name: f
                            for f in session.query(File)
                            .where(File.collection_name == db_collection.name)
                            .all()
                        }

                        if mainfolder.lower() != "welcome pack":
                            mainfolder = " ".join(mainfolder.strip().split()[:-1])

                        mainfolder = DOWNLOADS.joinpath(mainfolder)
                        os.makedirs(mainfolder, exist_ok=True)

                        categories = set()
                        syllabus = driver.find_element(
                            By.XPATH, '//*[@id="section-product_syllabus"]/div/div'
                        )
                        for element in syllabus.find_elements(
                            By.CLASS_NAME, "syllabus__item"
                        ):
                            for href in element.find_elements(By.TAG_NAME, "a"):
                                if href.text != "Show More":
                                    categories.add(href.get_attribute("href"))

                        categories = sorted(categories)

                        for category in categories:
                            self.driver_get(driver, category)

                            try:
                                downloads = driver.find_element(
                                    By.CLASS_NAME, "downloads"
                                ).find_elements(By.TAG_NAME, "a")
                            except NoSuchElementException:
                                continue

                            subfolder = driver.find_element(
                                By.CLASS_NAME, "panel__title"
                            ).text
                            subfolder.replace('"', "'")
                            path = mainfolder.joinpath(subfolder)
                            os.makedirs(path, exist_ok=True)

                            for d in downloads:
                                url = d.get_attribute("href")
                                name = Path(d.text)
                                if not name.suffix:
                                    # Going on an assumption here, but so far not incorrect
                                    name = d.text + ".png"
                                filepath = path.joinpath(name)
                                try:
                                    db_file = collection_files[filepath.name]
                                except KeyError:
                                    db_file = None

                                if db_file:
                                    if db_file.downloaded:
                                        continue
                                    elif (today - db_file.changed).days > 0:
                                        session.execute(
                                            delete(File).filter(
                                                (File.name == filepath.name)
                                                & (
                                                    File.collection_name
                                                    == db_collection.name
                                                )
                                            )
                                        )
                                        session.commit()
                                        db_file = None

                                if db_file is None:
                                    db_file = File(
                                        name=filepath.name,
                                        url=url,
                                        collection_name=db_collection.name,
                                        path=str(filepath.resolve()),
                                        changed=now(),
                                    )
                                    session.add(db_file)
                                    try:
                                        session.commit()
                                    except IntegrityError:
                                        pass

                                self.logger.info("Found %s", filepath.name)

    def find_and_write_data(self):
        try:
            self.get_data()
        except WebDriverException as err:
            logging.warning(str(err))
            update_chromedriver()
            self.get_data()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(threadName)s - %(name)s: %(message)s",
    )

    Base.metadata.create_all(engine)

    retriever = LootStudios(engine)
    retriever.find_and_write_data()
    download_all(engine)
    dropbox.find_files_and_start_upload(engine, "Loot Studios")
