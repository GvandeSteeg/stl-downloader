import os
from multiprocessing import Pool, cpu_count
from pathlib import Path

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import CommitInfo, ListFolderError, UploadSessionCursor
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from tqdm import tqdm

from stl_downloader import DOWNLOADS
from stl_downloader.database import File, engine, initializer


def upload(
    file_path: Path,
    collection: str,
    target_path: Path,
    chunk_size=4 * 1024 * 1024,
):
    with dropbox.Dropbox(
        app_key=os.environ["DROPBOX_APP_KEY"],
        oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
    ) as dbx:
        try:
            existing_files = {
                i.name for i in dbx.files_list_folder(str(target_path.parent)).entries
            }
        except ApiError as err:
            if isinstance(err.args[1], ListFolderError):
                existing_files = {}
            else:
                raise

        target_path = str(target_path)
        with open(file_path, "rb") as f, Session(engine) as session:
            base_name = file_path.name
            if base_name in existing_files:
                print(base_name, "already exists. Skipping.")
                db_file = (
                    session.query(File)
                    .filter(
                        (File.name == file_path.name)
                        & (File.collection_name == collection)
                    )
                    .one()
                )
                db_file.uploaded = True
                session.commit()
                return

            file_size = os.path.getsize(file_path)
            if file_size <= chunk_size:
                print(dbx.files_upload(f.read(), target_path))
            else:
                with tqdm(total=file_size, desc=file_path.name) as pbar:
                    upload_session_start_result = dbx.files_upload_session_start(
                        f.read(chunk_size)
                    )
                    pbar.update(chunk_size)
                    cursor = UploadSessionCursor(
                        session_id=upload_session_start_result.session_id,
                        offset=f.tell(),
                    )
                    commit = CommitInfo(path=target_path)
                    while f.tell() < file_size:
                        if (file_size - f.tell()) <= chunk_size:
                            dbx.files_upload_session_finish(
                                f.read(chunk_size), cursor, commit
                            )
                            db_file = (
                                session.query(File)
                                .filter(
                                    (File.name == file_path.name)
                                    & (File.collection_name == collection)
                                )
                                .one()
                            )
                            db_file.uploaded = True
                            session.commit()
                            print(base_name, "uploaded.")

                        else:
                            dbx.files_upload_session_append_v2(
                                f.read(chunk_size), cursor
                            )
                            # noinspection PyUnresolvedReferences,PyDunderSlots
                            cursor.offset = f.tell()
                            pbar.update(chunk_size)


def start_upload(filepath: Path, collection: str, site_name: str):
    p = filepath
    outpath = Path(
        "/" + site_name,
        p.relative_to(*p.parts[: p.parts.index(str(DOWNLOADS)) + 1], ""),
    )
    upload(
        filepath,
        collection,
        outpath,
    )


def find_files_and_start_upload(engine: Engine, site_name: str):
    with Session(engine) as session:
        total_files = (
            session.query(File)
            .filter((File.downloaded.is_(True)) & (File.uploaded.is_(False)))
            .all()
        )

    with Pool(cpu_count(), initializer) as pool:
        pool.starmap(
            start_upload,
            (
                (Path(db_file.path), db_file.collection_name, site_name)
                for db_file in total_files
            ),
        )
