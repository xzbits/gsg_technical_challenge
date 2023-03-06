import requests
from config_parser import config_parser
import os
import pathlib
import json
import time


def make_subdir(path, dir_name):
    path_dir_name = os.path.join(path, dir_name)
    try:
        os.mkdir(path_dir_name)
    except OSError:
        print("Creation of the temperature with time directory %s failed" % path_dir_name)
    else:
        print("Successfully created the  temperature with time directory %s" % path_dir_name)


def search(page, url, per_page=100):
    """
    Get bikes data
    :param page: specified page
    :param url: The bikes data requested url
    :param per_page: Number of bike per page
    :return: The bikes data
    """
    params = {
        ('page', str(page)),
        ('per_page', str(per_page)),
        ('stolenness', 'all')
    }
    response = requests.get(url, params=params, verify=False)

    return response.json()


def bike(bike_id, url, retries=3, waiting_time=2):
    """
    Get operations about bikes data

    :param bike_id: bike id number
    :param url: The operations about bikes data requested url
    :param retries: Number of retries after failed response
    :param waiting_time: The waiting time between each retry
    :return: The operations about bikes data
    """

    count = 1
    while count < retries:
        response = requests.get(url.format(bike_id))
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            print("Current URL is not found, retry after {}s".format(waiting_time))
            time.sleep(waiting_time)
        count += 1
    raise ValueError("Current URL did not response properly after {} retries! "
                     "Please check. URL: {}".format(retries, url))


def collect_data():
    dir_path = pathlib.Path().resolve()
    make_subdir(dir_path, "database")
    url_dict = config_parser('prj-config.cfg', 'bike_url')

    page = 1
    bikes_data = search(page, url_dict['bikes'])
    while len(bikes_data['bikes']) != 0:
        bikes_op_page = []
        for one_bike in bikes_data['bikes']:
            bike_op = bike(one_bike['id'], url_dict['op_bike'])
            bikes_op_page.append(bike_op['bike'])

        # Load bikes records
        bikes_filename = os.path.join(dir_path, 'database', "bikes_op_page_{}.json".format(page))
        with open(bikes_filename, "w", encoding="utf-8") as file:
            json.dump(bikes_op_page, file, ensure_ascii=False)

        # Update next loop
        page += 1
        bikes_data = search(page, url_dict['bikes'])


if __name__ == "__main__":
    collect_data()
