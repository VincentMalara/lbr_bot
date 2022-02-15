import xmltodict
translator = str.maketrans({chr(10): '', chr(9): ''})


def get_xml_content(filepath):
    with open(filepath, 'r', encoding='utf-8') as myfile:
        obj = xmltodict.parse(myfile.read())
    return obj

def extract_table(page, month, year):
    AA = page.find('table', {"class": "commonTable"})
    rows = []
    for i in AA.find_all('tr'):
        row = i.find_all('td')
        if len(row) > 6:
            date = row[0].text.strip()
            codeRESA = row[1].text.translate(translator).strip()
            name = row[2].text.translate(translator).strip()
            url = row[5].find('a')['href']
            rows.append(
                {'date': date, 'codeRESA': codeRESA, 'name': name, 'url': url, 'month': month, 'year': year})
    return rows