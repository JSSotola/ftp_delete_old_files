from ftplib import FTP_TLS, error_perm
from time import sleep
import socket

MAX_SIZE = 5  # GB
KEEP_RESERVE = 0.1  # keeps some percentage as free space
CHECK_EVERY = 3  # hours
RUN_ONLY_ONCE = True
ARMED = True
VERBOSE = False

with open("secrets", "r") as f:
    secrets = f.read()

host, user, passwd = secrets.split()


def to_MB(size_in_bytes):
    return round(size_in_bytes / (1024 * 1024), 2)


def get_all_files(ftp):
    file_list = {}
    ftp.cwd("/")
    directories = ftp.nlst()
    for directory in directories:
        ftp.cwd("/")
        files = []
        try:
            ftp.cwd(directory)
        except error_perm:
            continue
        ftp.dir(files.append)
        for file in files:
            file = file.split()
            file_list[directory + "/" + file[-1]] = int(file[4])
    return file_list


def ftp_check_size():
    print("Connecting to Wedos over FTP...")
    try:
        with FTP_TLS(host=host, user=user, passwd=passwd) as ftp:
            print("Connected, calculating total size...")
            file_dict = get_all_files(ftp)
            total_size = sum(file_dict.values())
            print("Total size is ", to_MB(total_size))

    except ConnectionAbortedError as e:
        print(e)
        print("Connection aborted. Restarting...")
        ftp_check_size()

    except socket.gaierror as e:
        print(e)
        print("Connection failed.")


ftp_check_size()
