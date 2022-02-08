import re

from bs4 import BeautifulSoup

from src.utils.set_logger import main as set_logger


logger = set_logger()


def get_numbers(x):
    if isinstance(x, str):
        y = re.findall('[0-9]+', x)
        y = sum([int(x) for x in y])
    else:
        y = ''
        logger.debug(f"get a number needs a string as input, {type(x)} has been given instead")
        print(f"get a number needs a string as input, {type(x)} has been given instead")
    return y


def answer_question(question):
     if isinstance(question, str):
         #print(question.strip())
         if "somme" in question:
             answer = get_numbers(question)
             #print(f"answer: {answer}")
             logger.info(f"answer to addition question: {answer}")
         else:
             if "moitié" in question:
                 answer = int(get_numbers(question) * 0.5)
                 #print(f"answer: {answer}")
                 logger.info(f"answer to moitié question: {answer}")
             else:
                 print(f"question unknown : {question}")
                 logger.debug(f"question unknown : {question}")
                 answer = 0
     else:
         logger.debug(f"question is not a string, {type(question)} has been given instead")
         print(f"question is not a string, {type(question)} has been given instead")
         answer = 0

     return answer


def check_page(driver, check_phrase, type_):
    #print(' - check_page')
    try:
        page_content = BeautifulSoup(driver.page_source, features="html.parser")
        found = ' - '.join([a.get_text().lower() for a in page_content.find_all(type_)])
        #print(found)
        #print(check_phrase.lower())
        page_status = (check_phrase.lower() in found)
        if not page_status:
            pass
            #print(f'{check_phrase} not found in {type_}')
            #logger.debug(f'{check_phrase} not found in {type_}')
    except Exception as e:
        #print(f'Not connected to {check_phrase} properly, error : {e}')
        #logger.debug(f'Not connected to {check_phrase} properly, error : {e}')
        page_status = False
    return page_status