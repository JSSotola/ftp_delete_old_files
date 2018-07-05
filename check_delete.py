from ftplib import FTP_TLS, error_perm
from time import sleep
import socket
from collections import defaultdict


MAX_SIZE = 5  # GB
MAX_FILE_COUNT = 5000
MAX_DIR_COUNT = 1000
KEEP_RESERVE = 0.1  # keeps free space
CHECK_EVERY = 2  # hours
RUN_ONLY_ONCE = True
ARMED = True
VERBOSE = False

run_counter = 0

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

        print(directory, len(files), "files,", to_MB(s), "MB", end="     ")
        print("Data from:", str(dates_dict)[27:-1])

    return file_list, dict_directories


def delete_old_files(ftp, files, size_to_delete, count_to_delete):
    file_keys = list(files.keys())
    file_keys.sort(key=lambda x: x.split("/")[1])
    deleted_size, deleted_n = 0, 0
    print("Oldest file is", file_keys[0], "and newest file is", file_keys[-1], ".")
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


def ftp_check_size():
    run_counter +=1
    try:
        with FTP_TLS(host=host, user=user, passwd=passwd) as ftp:
            print("Connected. Looking for files.")
            file_dict, dir_dict = get_all_files(ftp)

            print("\nChecking file counts in directories.")
            for directory, files in dir_dict.items():
                print(directory, "has", len(files), "files.", end=" ")
                if MAX_DIR_COUNT < len(files):
                    print("That is", len(files) - MAX_DIR_COUNT, "too too many. Deleting...", end=" ")
                    deleted_n = delete_old_in_dir(ftp, files, directory, len(files) - MAX_DIR_COUNT)
                    print("Deleted", deleted_n, end=" ")
                print("")

            print("\nChecking total size and count.")
            total_size = sum(file_dict.values())
            print("In total", len(file_dict), "files,", to_MB(total_size), "MB. ")
            size_over_limit = total_size - MAX_SIZE * 1024 * 1024 * 1024 * (1 - KEEP_RESERVE)
            count_over_limit = len(file_dict) - MAX_FILE_COUNT * (1 - KEEP_RESERVE)
            if size_over_limit > 0 or count_over_limit > 0:
                print("Too big or too many files. Deleting files...")
                deleted_size, deleted_n = delete_old_files(ftp, file_dict, size_over_limit, count_over_limit)
                print("Deleted", deleted_n, " files", to_MB(deleted_size), "MB in total. Current size is",
                      to_MB(total_size))
            else:
                print("Size is fine.")

    except (ConnectionAbortedError, error_perm) as e:
        print(e)
        print("Connection aborted. Restarting...")
        if run_counter < 100:
            ftp_check_size()


    except socket.gaierror as e:
        print(e)
        print("Connection failed.")
    run_counter = 0

while __name__ == "__main__":
    ftp_check_size()
    if RUN_ONLY_ONCE:
        print("\nFinished.")
        break
    print("\nWill check again in ", CHECK_EVERY, "hour(s).")
    sleep(CHECK_EVERY * 3.6 * 10 ** 6)
