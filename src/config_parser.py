import configparser


def config_parser(config_filepath, section):
    """
    Get required info dict

    :param config_filepath: Path points to config file
    :param section: section need to extract
    :return: Params dict
    """
    parser = configparser.ConfigParser()
    parser.read(config_filepath)

    result = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            result[param[0]] = param[1]
    else:
        raise Exception('{} does not have section {}'.format(config_filepath, section))

    return result
