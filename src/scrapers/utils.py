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
        page_status = check_page_simple(page_content,check_phrase, type_)
        if not page_status:
            pass
            #print(f'{check_phrase} not found in {type_}')
            #logger.debug(f'{check_phrase} not found in {type_}')
    except Exception as e:
        print(f'Not connected to {check_phrase} properly, error : {e}')
        #logger.debug(f'Not connected to {check_phrase} properly, error : {e}')
        page_status = False
    return page_status


def check_page_simple(page_content,check_phrase, type_):
    found = ' - '.join([a.get_text().lower() for a in page_content.find_all(type_)])
    return (check_phrase.lower() in found)

def find_rcs_in_page(page_content, rcs):
    foundRCS=''
    regex = r'[ABCDEFGHIJKLM]\d+'
    x = ' '.join([a.get_text() for a in page_content.find_all('h1')])
    foundRCS = re.findall(regex, x)
    if len(foundRCS)>0:
        print(f"found RCS is  : {foundRCS[0]}, expected is : {rcs}")
        foundRCS = foundRCS[0]
    else:
        print(f"found RCS is empty, expected is : {rcs}")
    return foundRCS


def scrap_page_check(page_content, rcs):
    print('----  checkrbepage ----')
    output = {
        'test_err': check_page_simple(page_content, "Les erreurs suivantes ont été détectées", 'b'),
        'test_err2': check_page_simple(page_content,"élément(s) trouvé(s)", 'span'),
        'test_rcs': check_page_simple(page_content, rcs, 'h1'),
        'testvalid1': check_page_simple(page_content, "Bénéficiaires effectifs", 'h1'),
        'testvalid2': check_page_simple(page_content, "Date de la dernière déclaration", 'h2'),
        'foundRCS': find_rcs_in_page(page_content, rcs)
    }
    print(output)
    return output
