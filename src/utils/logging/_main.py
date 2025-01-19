import functools
import time 
from datetime import timedelta
import logging

__all__ = ['logger', 'sign_log', 'perf_log']


# Define the logger

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('main.log', mode='w', encoding='ascii')
formatter = logging.Formatter('%(asctime)s %(filename)s %(name)s %(levelname)s %(message)s')

fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

logger.addHandler(fh)



def sign_log(func):
    """ Decorator function which logs the runtime signature of the function """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug(f'{func.__name__} with args={(args, kwargs)}')
        return func(*args, **kwargs)
        
    return wrapper

def perf_log(func):

    """Decorator function which logs the start/end and total time taken for exectuion of the function"""
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        logger.debug(f'{func.__name__} execution start')

        start = time.perf_counter()

        func(*args, **kwargs)

        end = time.perf_counter()

        logger.debug(f'{func.__name__} execution end')

        total_seconds = round(end-start, 3)
        td = timedelta(seconds=total_seconds)

        logger.debug(f'total execution time for {func.__name__} : {str(td)}')

    return wrapper