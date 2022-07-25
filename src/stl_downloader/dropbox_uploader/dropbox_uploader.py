import os
from multiprocessing import Pool
from pathlib import Path

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import CommitInfo, ListFolderError, UploadSessionCursor
from tqdm import tqdm


def upload(
        access_token,
        file_path,
        target_path,
        timeout=900,
        chunk_size=4 * 1024 * 1024,
):
    dbx = dropbox.Dropbox(access_token, timeout=timeout)

    try:
        existing_files = {
            i.name for i in dbx.files_list_folder(os.path.dirname(target_path)).entries
        }
    except ApiError as err:
        if isinstance(err.args[1], ListFolderError):
            existing_files = {}
        else:
            raise
    base_name = os.path.basename(file_path)
    if base_name in existing_files:
        print(file_path, "already exists. Skipping.")
        return

    with open(file_path, "rb") as f:
        print("Uploading", file_path)
        file_size = os.path.getsize(file_path)
        if file_size <= chunk_size:
            print(dbx.files_upload(f.read(), target_path))
        else:
            with tqdm(total=file_size, desc=os.path.basename(file_path)) as pbar:
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
                        dbx.files_upload_session_finish(f.read(chunk_size), cursor, commit)
                        print(file_path, "uploaded.")

                    else:
                        dbx.files_upload_session_append_v2(f.read(chunk_size), cursor)
                        # noinspection PyUnresolvedReferences,PyDunderSlots
                        cursor.offset = f.tell()
                        pbar.update(chunk_size)


def start_upload(path: str, prefix: str):
    outpath = path.replace(prefix, "")
    upload(
        os.environ["DROPBOX_ACCESS_TOKEN"],
        path,
        outpath,
    )


def find_files_and_start_upload(folder: Path):
    total_files = []
    for root, dirs, files in os.walk(folder.resolve()):
        if files:
            total_files.extend([os.path.join(root, file) for file in files])

    with Pool(25) as p:
        p.map(start_upload, ((path, folder) for path in total_files))
