import re


translator = str.maketrans({chr(10): '', chr(9): ''})
urlstring=r"https:\/\/gd\.lu\/rcsl\/\w{3,}"
three_char_re = re.compile(urlstring)


def clean_spaces(x):
    if isinstance(x, str):
        x = (' '.join(x.split()).strip()).translate(translator)
    return x

def get_pdflink(x):
    return three_char_re.search(str(x))[0]

def replaced_RCS(x):
    y = ""
    name = x
    if isinstance(x, str):
        if " Le numéro RCS saisi a été remplacé par le numéro RCS" in x:
            y = x.split(" Le numéro RCS saisi a été remplacé par le numéro RCS ")[1]
            regex = r'[ABCDEFGHIJKLM]\d+'
            bb = re.findall(regex, x)
            if len(bb) > 0:
                y = bb[0]
                name = x.split(y)[0].strip()

    return y, name