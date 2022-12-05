import logging
import os
import re
import shutil
import ssl
import subprocess
import urllib.parse
import urllib.request as urlrequest
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path
from urllib.error import URLError

import dotenv
from chromedriver_py import binary_path
from retry import retry
from selenium import webdriver
from selenium.common import WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
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
            f"chromedriver-py>={main_chrome_version},<{int(main_chrome_version) + 1}",
        ],
        check=True,
    )


def download(url: str, filepath: Path, collection: str):
    os.makedirs(filepath.parent, exist_ok=True)
    filename = filepath.name
    print(f"Downloading {filename} from {collection}")
    try:
        with urlrequest.urlopen(url) as response, open(filepath, "wb") as out_file:
            shutil.copyfileobj(response, out_file)
    except URLError as err:
        print(err, "-", filename)
    else:
        with Session(engine) as session:
            db_file = (
                session.query(File)
                .filter((File.name == filename) & (File.collection_name == collection))
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
            self.driver_get(driver, "https://lootstudios.com/login")
            driver.find_element(By.CLASS_NAME, "login-username").find_element(
                value="user_login"
            ).send_keys(os.environ["EMAIL"])
            driver.find_element(By.CLASS_NAME, "login-password").find_element(
                value="user_pass"
            ).send_keys(os.environ["LOOT_PASSWORD"].strip())
            driver.execute_script('jQuery("#loginform").submit()')

            self.driver_get(driver, f"https://lootstudios.com/my-loots")

            collections = sorted(
                {
                    l.get_attribute("href")
                    for l in driver.find_element(
                        By.XPATH, "/html/body/main/section/div/div[3]"
                    ).find_elements(By.TAG_NAME, "a")
                }
            )

            for collection in collections:
                # Promotional pages can also appear in collections, so need to skip those as they don't hold data
                if "loot/" not in collection:
                    continue

                if collection in to_skip:
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
                        "/html/body/main/section[2]/div/div[1]/div/div[1]/h2",
                    ).text.split("\n")[0]

                    db_collection.name = mainfolder
                    session.commit()

                    collection_files = {
                        f.name: f
                        for f in session.query(File)
                        .where(File.collection_name == db_collection.name)
                        .all()
                    }

                    mainfolder = DOWNLOADS.joinpath(mainfolder)
                    os.makedirs(mainfolder, exist_ok=True)

                    html = driver.page_source
                    zip_files = set(
                        re.findall(
                            r"https://storage\.googleapis\.com.*?\.zip", html, re.I
                        )
                    )
                    maps = {
                        m
                        for m in re.findall(
                            r"https://lootstudios\.com.*?\.zip", html, re.I
                        )
                        if not "storage" in m
                    }
                    jpg_files = set(
                        re.findall(r"https://lootstudios\.com.*?\.jpg", html, re.I)
                    )

                    download_files = zip_files | maps | jpg_files
                    for dl_file in download_files:
                        safe_url = dl_file.replace(" ", r"\ ")
                        normalised_url = urllib.parse.unquote(dl_file)
                        if (
                            "Download" in normalised_url
                        ):  # A "Download All" folder, ignore
                            continue

                        if "googleapis" in dl_file and dl_file.endswith(
                            "zip"
                        ):  # This is a STL file
                            relative_url = normalised_url.split("bucket/")[-1].split(
                                "/"
                            )

                            filepath = mainfolder.joinpath(Path(*relative_url[1:]))

                        elif "lootstudios" in dl_file and dl_file.endswith(
                            ("zip", "jpg")
                        ):  # Map or encounter
                            filepath = mainfolder.joinpath("Misc", Path(dl_file).name)

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
                                        & (File.collection_name == db_collection.name)
                                    )
                                )
                                session.commit()
                                db_file = None

                        if db_file is None:
                            db_file = File(
                                name=filepath.name,
                                url=safe_url,
                                collection_name=db_collection.name,
                                path=str(filepath.resolve()),
                                changed=now(),
                            )
                            session.add(db_file)
                            try:
                                session.commit()
                            except IntegrityError:
                                session.rollback()

                                # Try again by prepending folder to filename
                                new_name = "_".join((relative_url[-2], filepath.name))
                                db_file = File(
                                    name=new_name,
                                    url=safe_url,
                                    collection_name=db_collection.name,
                                    path=str(filepath.resolve()),
                                    changed=now(),
                                )
                                session.add(db_file)
                                session.commit()

                            self.logger.info("Found %s", filepath.name)

    def find_and_write_data(self):
        try:
            self.get_data()
        except WebDriverException as err:
            logging.warning(str(err))
            update_chromedriver()


def delete_finished_uploads(engine):
    # TODO add deleted column
    with Session(engine) as session:
        done = session.query(File).filter(File.uploaded.is_(True)).all()

        for f in done:
            if os.path.exists(f.path):
                os.remove(f.path)


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
    delete_finished_uploads(engine)
