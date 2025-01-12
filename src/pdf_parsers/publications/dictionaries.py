
list_bilan=['ABRIDGED BALANCE SHEET','VERKÜRZTE BILANZ', 'BALANCE SHEET', 'BILAN ABRÉGÉ','BILANZ','BILAN']

list_abrege=['ABRIDGED','VERKÜRZTE', 'ABRÉGÉ']

list_CR=['COMPTE DE PROFITS ET PERTES ABRÉGÉ', 'ABRIDGED PROFIT AND LOSS ACCOUNT', 'VERKÜRZTE GEWINN- UND VERLUSTRECHNUNG', 'GEWINN- UND VERLUSTRECHNUNG','COMPTE DE PROFITS ET PERTES', 'PROFIT AND LOSS ACCOUNT' ]

list_bilan = [str.lower(x) for x in list_bilan]
list_abrege = [str.lower(x) for x in list_abrege]
list_CR = [str.lower(x) for x in list_CR]

splitters = ['annexe aux comptes annuels', 'Comptes annuels pour l’exercice','annexe aux comptes au', 'anhang für die veröffentlichung', 'notes to the annual accounts', 'notes to the accounts', 'general information']

OBJECTS_DICTS = {
    'Objet social': {'indic': 'Objet social (indication)', 'incomp': 'Objet social incomplet'},
    'Objet': {'indic': 'Objet (indication)', 'incomp': "Objet incomplet"},
    'Objet du commerce': {'indic': 'Objet du commerce', 'incomp': "Objet du commerce incomplet"},
    'Zweck der Gesellschaft': {'indic': 'Zweck der Gesellschaft (Kurzfassung)',
                               'incomp': "Unvollständiger Zweck der Gesellschaft"},
    'Tätigkeiten': {'indic': 'Tätigkeiten (Kurzfassung)', 'incomp': "Unvollständige Tätigkeiten"},
    'Activités': {'indic': 'Activités (indication)', 'incomp': "Activités incomplètes"},
    'Zweck': {'indic': 'Zweck (Kurzfassung)', 'incomp': "Unvollständiger Zweck"}
}


DICT_PERSONNE = {
    'ADM': {'regex': r"\d+ Nouvel administrateur / gérant", 'label': 'Administrateur(s) / Gérant(s)'},
    'DGJ': {'label': 'Délégué(s) à la gestion journalière', 'regex': r"\d+ Nouveau délégué à la gestion journalière"},
    'AS': {'label': 'Associé(s) solidaire(s)', 'regex': r"\d+ Nouvel associé solidaire"},
    'PCCC': {'label': 'Personne(s) chargée(s) du contrôle des comptes',
           'regex': r"\d+ Nouvelle personne chargée du contrôle des comptes"},
    'RPAS': {'label': 'Représentant(s) permanent(s) pour l activité de la succursale',
           'regex': r"\d+ Nouveau représentant permanent"},
    'PD': {'label': 'Président / directeur(s)', 'regex': r"\d+ Nouveau président / directeur"},
    'PAPE': {'label': 'Personne(s) ayant le pouvoir d engager la société',
           'regex': r"\d+ Nouvelle personne ayant le pouvoir d'engager la société"},
    'AC': {'label': 'Associé(s) commandité(s)', 'regex': r"\d+ Nouvel associé commandité"},
    'SG': {'label': 'Société de gestion', 'regex': r"(\d+ )*Nouvelle société de gestion"},
    'PAG': {'label': 'Personne(s) autorisée(s) à gérer, administrer et signer', 'regex': r"(\d+ )*Nouvelle personne autorisée à gérer, administrer et signer"},
    'ASSO': {'regex': r"\d+ Nouvel associé", 'label': 'Associé(s)'},
    'GER': {'regex': r"\d+ Nouveau gérant", 'label': 'Gérant(s)'}

}

# modif for EY 26/11/2021
PERSON_MERGED = {
    "Administrateur(s) / Gérant(s)": "Gérant/Administrateur",
    "Représentant(s) permanent(s) pour l’activité de la succursale": "Gérant/Administrateur",
    "Président / directeur(s)": "Gérant/Administrateur",
    "Gérant(s)": "Gérant/Administrateur",
    "Délégué(s) à la gestion journalière": "Délégué à la gestion journalière",
    "Personne(s) ayant le pouvoir d’engager la société": "Délégué à la gestion journalière",
    "Personne(s) autorisée(s) à gérer, administrer et signer": "Délégué à la gestion journalière",
    "Associé(s) solidaire(s)": "Actionnaire/Associé",
    "Associé(s) commandité(s)": "Actionnaire/Associé",
    "Associé(s)": "Actionnaire/Associé",
    "Personne(s) chargée(s) du contrôle des comptes": "Personne(s) chargée(s) du contrôle des comptes",
    "Société de gestion": "Société de gestion",
}

PERSON_MERGED_REVERSED = {
    "Gérant/Administrateur":["Administrateur(s) / Gérant(s)",
                             "Représentant(s) permanent(s) pour l’activité de la succursale",
                             "Président / directeur(s)", "Gérant(s)"],
    "Délégué à la gestion journalière":["Délégué(s) à la gestion journalière", "Personne(s) ayant le pouvoir d’engager la société",
                                        "Personne(s) autorisée(s) à gérer, administrer et signer"],
    "Actionnaire/Associé":["Associé(s) solidaire(s)", "Associé(s) commandité(s)"],
    "Personne(s) chargée(s) du contrôle des comptes": ["Personne(s) chargée(s) du contrôle des comptes"],
    "Société de gestion": ["Société de gestion"]}



# endmodif for EY 26/11/2021


DICT_PERSONNE_SPLITTERS = {
    'regex1':  r"^\d+ (.+ )+(p|P)age \d+ \✔$",
    'regex2':  r"^@@\d+ ", #@@ added to be sure to replace only first occurence
    'regex3':  r"[ ]*Page \d+ / \d+$", #[ ]* added instead of simple ' '
    'page':  'page',
    'raye':  "Modifier Rayer ✔",
    'modifie':  "Modifier ✔ Rayer",
    'demission':  "✔ Démission d",
}



DICT_DONNEES_A_MODIF = {
    'regex': r" page \d+$",
    'startlist': ['Données à modifier', 'Données à inscrire', 'Types de mandats concernés par la modification', 'Types de personne concernée par la démission'],
    'page2': 'Page 2 / ',
    'page': "page "}

LABEL_DICT = {
    'Siège social': ['Numéro', 'Rue', 'Code postal', 'Localité'],
    'Dénomination ou raison sociale': ['Dénomination ou raison sociale', 'Le cas échéant, abréviation utilisée'],
    'Durée': ['Durée', 'Date de fin'],
    'Capital social / Fonds social': ['Type', 'Montant', 'Devise', 'Etat de libération', 'Pourcentage, le cas échéant'],
    'Forme juridique': ['Forme juridique', 'Mention supplémentaire (le cas échéant)'],
    'Date de constitution': ['Date de constitution'],
    'Date de création': ['Date de création'],
    'Date de création du commerce': ['Date de création du commerce'],
    'Nom du fonds': ['Nom du fonds'],
    'Autorisation(s)': ['Type', 'Numéro', 'Date', 'Délivré(e) par'],
    'Enseigne(s) commerciale(s)': ['Enseigne(s) commerciale(s) Le cas échéant, abréviation(s) utilisée(s)'],
    "Adresse où s'exerce l'activité commerciale": ['Numéro', 'Rue (Veuillez indiquer le nom complet de la rue comme par exemple : Rue de la gare)', 'Code postal', 'Localité'],
    'Données personnelles': ['Nom',  'Prénom(s)',  'Date de naissance',  'Lieu de naissance',  'Pays de naissance',  'Nationalité',  'Sexe']
}

#list_personne = [DICT_PERSONNE[i]['label'] for i in DICT_PERSONNE.keys()]

list_personne = [
            "Gérant/Administrateur", "Délégué à la gestion journalière", "Actionnaire/Associé",
            "Personne(s) chargée(s) du contrôle des comptes", "Société de gestion"
                           ]

ff={
"Bezeichnung der Gesellschaft oder Firmenname" : "Dénomination ou raison sociale",
"Handelsbezeichnung(en)" : "Enseigne(s) commerciale(s)",
"Rechtsform" :"Forme juridique",
"Sitz der Gesellschaft" :"Siège social",
"Zweck" :"Objet",
"Dauer der Gesellschaft" :"Durée",
"Geschäftsjahr" :"Exercice social",
"Komplementär(e)" :"Komplementär(e)",
"Geschäftsführer" :"Gérant(s)",
"Freiwillige Liquidation" :"Liquidation volontaire"
}


REPLACE_PHRASE = {
    "Geschäftsführer" :"Gérant(s)",
    "Freiwillige Liquidation" :"Liquidation volontaire",
    'GFDCXGF': 'Données personnelles ....à traduire',
    'XSXSXZZ':'Date de création du commerce',
    'XSXSXZZ1':"Objet du commerce incomplet",
    'XSXSXZZ2':"Objet du commerce",
    'XSXSXZZ3':'Enseigne(s) commerciale(s)',
    'XSXSXZZ4':"Adresse où s'exerce l'activité commerciale",
    'Neuer Verwaltungs- und Zeichnungsberechtigter': "Nouvelle personne autorisée à gérer, administrer et signer",
    'Verwaltungs- und Zeichnungsberechtigte(r)': 'Personne(s) autorisée(s) à gérer, administrer et signer',
    "Vorstandsmitglied(er) / Geschäftsführer"	:	"Administrateur(s) / Gérant(s)",
    "Teilhaber der Gesellschaft": "Associé(s)",
    "Tägliche Geschäftsführung": "Délégué(s) à la gestion journalière",
    "YYY": "Associé(s) solidaire(s)",
    "Prüfungsbeauftragter der Geschäftsbuchführung"	: "Personne(s) chargée(s) du contrôle des comptes",
    "Handlungsbevollmächtigte(r) der Zweigniederlassung": "Représentant(s) permanent(s) pour l activité de la succursale",
    "Vorsitzender / Direktor(en)": "Président / directeur(s)",
    "XXX": "Personne(s) ayant le pouvoir d engager la société",
    "Komplementär(e)": "Associé(s) commandité(s)",
    "Verwaltungsgesellschaft": "Société de gestion",
    'Abzuändernde Angaben': 'Données à modifier',
    'Einzutragende Angaben': 'Données à inscrire',
    'Art des von der Änderung betroffenen Mandats': 'Types de mandats concernés par la modification',
    'Von der Rücktrittserklärung betroffener Personentyp':'Types de personne concernée par la démission',
    "Sitz der Gesellschaft": "Siège social",
    "Bezeichnung der Gesellschaft oder Firmenname": 'Dénomination ou raison sociale',
    "Allgemeinübliche Abkürzung": 'Le cas échéant, abréviation utilisée',
    'Dauer der Gesellschaft': 'Durée',
    'Ende der Gesellshaft': 'Date de fin',
    'Kapital der Gesellschaft': 'Capital social / Fonds social',
    'Gesellschaftskapital': 'Capital social / Fonds social',
    'Einzahlungsstand des gezeichneten Gesellschäftskapitals': 'Etat de libération',
    'Einzahlungsstand': 'Etat de libération',
    'Prozentsatz des Einzahlungsstandes': 'Pourcentage, le cas échéant',
    'Zusätzliche Angaben (gegebenenfalls)': 'Mention supplémentaire (le cas échéant)',
    'Gründungsdatum': 'Date de constitution',
    "Name des Fonds": 'Nom du fonds',
    'allgemeinübliche Abkürzung (ggf.)': 'Le cas échéant, abréviation utilisée',
    'Angaben zur Person': 'Données personnelles',
    'Privatadresse': 'Adresse privée',
    'Zivilstand': 'Etat civil',
    'Angaben zur Hauptniederlassung': "Données relatives à l'établissement principal",
    'Handelsbezeichnung(en)': 'Enseigne(s) commerciale(s)',
    'Geschäftsadresse': "Adresse où s'exerce l'activité commerciale",
    'Geschäftszweck': 'Objet du commerce',
    'Geschäftsführer / Generalbevollmächtigte(r)': 'Gérant(s) / fondé(s) de pouvoir général',
    'Handelsermächtigung(en)': 'Autorisation(s)',
    'Rechtsform': 'Forme juridique',
    'Zweck der Gesellschaft': 'Objet social',
    'Geschäftsjahr': 'Exercice social',
    'tägliche Geschäftsführung': 'Délégué(s) à la gestion journalière',
    'Prüfungsbeauftragte(r) der Geschäftsbuchführung': 'Personne(s) chargée(s) du contrôle des comptes',
    'Verschmelzung / Spaltung': 'Fusion / Scission',
    'freiwillige Liquidation': 'Liquidation volontaire',

    "Privatperson": 'Personne physique',
    "Juristische Person aus Luxemburg": "Personne morale luxembourgeoise",
    "Privat oder Berufsadresse" :'Adresse privée ou professionnelle',
    "Art des Mandats": 'Type de mandat',
    "Gesellschaftsorgan": 'Organe social',
    "Amtsausführung":'Fonction',
    "Zeichnungsberechtigung (Kurzfassung)": 'Pouvoir de signature (indication)',
    "Dauer des Mandats":'Durée du mandat',
    "Bestellungsdatum":'Date de nomination',
    #"Dauer des Mandats":'Durée du mandat',
    "Ablaufdatum des Mandats":"Date d'expiration du mandat",
    "bis zum Jahr, in dem die Generalversammlung stattfinden wird": "jusqu'à l'assemblée générale qui se tiendra en l'année",
    #"Privatperson" : 'Personne physique',
    #"Name":'Nom',
    "Vorname(n)":'Prénom(s)',
    "Geburtsdatum":"Date de naissance",
    "Geburtsort":"Lieu de naissance",
    "Geburtsland":"Pays de naissance",
    "Handelsregisternummer": "N° d'immatriculation au RCS",


    "Hausnummer Strasse": 'Numéro Rue',
    "Hausnummer": 'Numéro',
    'Strasse': 'Rue',
    'Postleitzahl': 'Code postal',
    'Ortschaft': 'Localité',
    'Zweck': 'Objet social',
    "Begrenzt": 'limitée',
    'Ganzeinzahlung': 'Total',
    'Kapitalbetrag': 'Montant',
    'betrag': 'Montant',
    'Kapitaldevise': 'Devise',
    'Währung': 'Devise',
    'Unbegrenzt': 'Illimitée',
    'unbegrenzt': 'Illimitée',
    'Art': 'Type',
    'Bezeichnung': 'Dénomination',
    "Sitz": "Siège",

    'Dauer': "Durée",
    "Name": 'Nom',


    #'Geschäftsführer'
    # 'Komplementär(e)'

}



REPLACE_WORD = {
    "Neuer Geschäftsführer": "Nouveau gérant", #????
    'Neuer Verwaltungs- und Zeichnungsberechtigter': "Nouvelle personne autorisée à gérer, administrer et signer",
    "Neues Vorstandsmitglied / Neuer Geschäftsführer":	"Nouvel administrateur / gérant",
    "Neuer Teilhaber": "Nouvel associé",
    "Neuer Tägliche Geschäftsführung": "Nouveau délégué à la gestion journalière" ,
    "LLLL": "Nouvel associé solidaire",
    "Neuer Prüfungsbeauftragter der Geschäftsbuchführung": "Nouvelle personne chargée du contrôle des comptes",
    "Neuer Handlungsbevollmächtigter der Zweigniederlassung": "Nouveau représentant permanent",
    "Neuer Vorsitzender / Direktor"	: "Nouveau président / directeur" ,
    "ZZZ": "Nouvelle personne ayant le pouvoir d'engager la société",
    "Neuer Komplementär": "Nouvel associé commandité"	,
    "Neue Verwaltungsgesellschaft":	"Nouvelle société de gestion"	,
    'seite': 'page',
    'Seite': 'Page',
    "Ändern löschen ✔": "Modifier Rayer ✔",
    "Ändern ✔ löschen": "Modifier ✔ Rayer",
   "Ändern ✔ Löschen": "Modifier ✔ Rayer",
    "✔ Rücktrittserklärung d": "✔ Démission d",
    "Ändern Löschen ✔": "Modifier Rayer ✔",
    "Ändern Löschen" : "Modifier Rayer",



}


TRAD_SPLITERS={
    "Komplementär(e)": "Associé(s) commandité(s)",
    'Abzuändernde Angaben': 'Données à modifier',
    'Angaben zur Person': 'Données personnelles',
    'Privatadresse': 'Adresse privée',
    'Zivilstand': 'Etat civil',
    'Angaben zur Hauptniederlassung': "Données relatives à l'établissement principal",
    'Handelsbezeichnung(en)': 'Enseigne(s) commerciale(s)',
    'Geschäftsadresse': "Adresse où s'exerce l'activité commerciale",
    'Geschäftszweck': 'Objet du commerce',
    'Geschäftsführer / Generalbevollmächtigte(r)': 'Gérant(s) / fondé(s) de pouvoir général',
    'Handelsermächtigung(en)': 'Autorisation(s)',
    'Einzutragende Angaben'	: 'Données à inscrire',
    'Bezeichnung der Gesellschaft oder Firmenname': 'Dénomination ou raison sociale',
    'Rechtsform': 'Forme juridique'	,
    'Sitz der Gesellschaft': 'Siège social',
    'Zweck der Gesellschaft': 'Objet social',
    'Zweck': 'Objet social',
    'Kapital der Gesellschaft': 'Capital social / Fonds social',
    'Gesellschaftskapital': 'Capital social / Fonds social',
    'Gründungsdatum': 'Date de constitution',
    'Dauer der Gesellschaft': 'Durée',
    'Geschäftsjahr': 'Exercice social',
    'Teilhaber der Gesellschaft': 'Associé(s)'	,
    'Vorstandsmitglied(er) / Geschäftsführer': 'Administrateur(s) / Gérant(s)',
    'tägliche Geschäftsführung': 'Délégué(s) à la gestion journalière',
    'Tägliche Geschäftsführung': 'Délégué(s) à la gestion journalière',
    'Prüfungsbeauftragte(r) der Geschäftsbuchführung': 'Personne(s) chargée(s) du contrôle des comptes'	,
    'Verschmelzung / Spaltung': 'Fusion / Scission',

    'Verwaltungs- und Zeichnungsberechtigte(r)': 'Personne(s) autorisée(s) à gérer, administrer et signer',

    "Verwaltungsgesellschaft":"Société de gestion",
    "Datum der Gründungsurkunde": "Date de constitution",
    "Datum der Geschäftseröffnung":"Date de création du commerce",
    "Mitglied(er) des Verwaltungsorgans":"Personne(s) autorisée(s) à gérer, administrer et signer",
    "Geschäftsführer":"Gérant(s)",



    'freiwillige Liquidation': 'Liquidation volontaire',
    'Bezeichnung': 'Dénomination',

    "Sitz": "Siège",
    'Dauer': "Durée",
    "Name des Fonds": 'Nom du fonds',
    "Name":'Nom',



# 'Geschäftsführer'
# 'Komplementär(e)'
}

SUBPERSOINFO_DICT = {
    'Adresse privée ou professionnelle': ['Numéro', 'Rue', 'Code postal', 'Localité', 'Pays' ],
    'Type de mandat': ['Organe social', 'Fonction', 'Pouvoir de signature (indication)'],
    'Parts sociales': ['Type(s) de parts (le cas échéant) Nombre de parts détenues'],
    'Durée du mandat': ['Date de nomination', 'Durée du mandat', "Date d'expiration du mandat", "jusqu'à l'assemblée générale qui se tiendra en l'année"],
    'Siège': ['Numéro', 'Rue', 'Code postal', 'Localité', 'Pays']
}



'''
    if list_[1] in ['Personne physique', "Personne morale luxembourgeoise", "Personne morale étrangère"]:


    'Adresse privée ou professionnelle': ['Numéro', 'Rue', 'Code postal', 'Localité', 'Pays' ],
    ],
    'Parts sociales': ['Type(s) de parts (le cas échéant) Nombre de parts détenues'],
    
    'Siège': ['Numéro', 'Rue', 'Code postal', 'Localité', 'Pays' ]
'''