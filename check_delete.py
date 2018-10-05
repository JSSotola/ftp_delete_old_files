from ftplib import FTP_TLS, error_perm
from time import sleep
import socket
from collections import defaultdict
import logging
import logging.handlers


MAX_SIZE = 5  # GB
MAX_FILE_COUNT = 5000
MAX_DIR_COUNT = 1000
KEEP_RESERVE = 0.1  # keeps free space
CHECK_EVERY = 2  # hours
RUN_ONLY_ONCE = True
ARMED = True
VERBOSE = False


LOG_FILENAME = "log.out"


def set_up_logging():

    # Set up a specific logger with our desired output level
    my_logger = logging.getLogger("Krizenec_delete_old")
    my_logger.setLevel(logging.DEBUG)

    # Add the log message handler to the logger
    handler = logging.handlers.RotatingFileHandler(
        LOG_FILENAME, maxBytes=1024 * 100, backupCount=1
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    my_logger.addHandler(handler)


def load_secrets():
    global host, user, passwd
    with open("secrets", "r") as f:
        secrets = f.read()

    host, user, passwd = secrets.split()


def to_MB(size_in_bytes):
    return round(size_in_bytes / (1024 * 1024), 1)


def get_all_files(ftp):
    file_list = {}
    ftp.cwd("/")
    directories = ftp.nlst()
    dict_directories = {}
    for directory in directories:
        dates_dict = defaultdict(int)
        files = []
        files_names = []
        s = 0

        ftp.cwd("/")
        try:
            ftp.cwd(directory)
        except error_perm:
            continue

        ftp.dir(files.append)

        for file in files:
            file = file.split()
            size = int(file[4])
            s += size
            dates_dict[file[-1][:8]] += 1
            file_list[directory + "/" + file[-1]] = size
            files_names += [file[-1]]

        dict_directories[directory] = files_names

        my_logger.info(f"{directory}{len(files)} files {to_MB(s)} MB")
        my_logger.info(f"Data from: {str(dates_dict)[27:-1]}")

    return file_list, dict_directories


def delete_old_files(ftp, files, size_to_delete, count_to_delete):
    file_keys = list(files.keys())
    file_keys.sort(key=lambda x: x.split("/")[1])
    deleted_size, deleted_n = 0, 0
    my_logger.info(f"Oldest file is {file_keys[0]} and newest file is {file_keys[-1]}.")
    while deleted_size < size_to_delete or deleted_n < count_to_delete:
        file_to_delete = file_keys[0]
        deleted_size += files.pop(file_to_delete)
        file_keys.pop(0)
        if ARMED:
            ftp.delete("/" + file_to_delete)
            deleted_n += 1
    return deleted_size, deleted_n


def delete_old_in_dir(ftp, files, directory, count_to_delete):
    files.sort()
    deleted_n = 0
    while deleted_n < count_to_delete:
        file_to_delete = files[0]
        files.pop(0)
        if ARMED:
            ftp.delete("/" + directory + "/" + file_to_delete)
            deleted_n += 1
    return deleted_n


def ftp_check_size(run_counter=0):
    run_counter += 1
    try:
        with FTP_TLS(host=host, user=user, passwd=passwd) as ftp:
            my_logger.info("Connected. Looking for files.")
            file_dict, dir_dict = get_all_files(ftp)

            my_logger.info("Checking file counts in directories.")
            for directory, files in dir_dict.items():
                my_logger.info(f"{directory} has {len(files)} files.")
                if MAX_DIR_COUNT < len(files):
                    my_logger.info(
                        f"That is {len(files) - MAX_DIR_COUNT} too too many. Deleting..."
                    )
                    deleted_n = delete_old_in_dir(
                        ftp, files, directory, len(files) - MAX_DIR_COUNT
                    )
                    my_logger.info(f"Deleted {deleted_n}")
                my_logger.info("")

            my_logger.info("Checking total size and count.")
            total_size = sum(file_dict.values())
            my_logger.info(f"In total {len(file_dict)} files {to_MB(total_size)}MB.")
            size_over_limit = total_size - MAX_SIZE * 1024 * 1024 * 1024 * (
                1 - KEEP_RESERVE
            )
            count_over_limit = len(file_dict) - MAX_FILE_COUNT * (1 - KEEP_RESERVE)
            if size_over_limit > 0 or count_over_limit > 0:
                my_logger.info("Too big or too many files. Deleting files...")
                deleted_size, deleted_n = delete_old_files(
                    ftp, file_dict, size_over_limit, count_over_limit
                )
                my_logger.info(
                    f"Deleted {deleted_n} files {to_MB(deleted_size)} MB in total. Current size is {to_MB(total_size)}"
                )
            else:
                my_logger.info("Size is fine.")

    except (ConnectionAbortedError, error_perm) as e:
        my_logger.error(e)
        my_logger.error("Connection aborted. Restarting...")
        if run_counter < 100:
            ftp_check_size(run_counter=run_counter)

    except socket.gaierror as e:
        my_logger.error(e)
        my_logger.error("Connection failed.")


if __name__ == "__main__":
    set_up_logging()
    my_logger.info("")
    my_logger.info("Starting..")
    load_secrets()
    ftp_check_size()

    my_logger.info("Finished.")
