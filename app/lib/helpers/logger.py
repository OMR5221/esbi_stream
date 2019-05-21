import logging

class Logger():

    def createLogger(logger_name, log_dir, log_filename):

        logger = logging.getLogger(str(logger_name))
        logger.setLevel(logging.DEBUG)

        log_filename = '{}.log'.format(str(log_filename))
        log_file_handler = logging.FileHandler(log_dir.joinpath(log_filename))
        log_file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_file_handler.setFormatter(formatter)
        logger.addHandler(log_file_handler)

        return logger
