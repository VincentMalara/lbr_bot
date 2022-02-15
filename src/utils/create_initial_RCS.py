
def create_exhaustive_list():
    # B - Sociétés commerciales : B1 à B254117
    RCSlist = ['B'+str(i) for i in range(265000)]
    # A - Commerçant personne physique : A1 à A42887
    RCSlist = RCSlist + ['A'+str(i) for i in range(44000)]
    # C - G.I.E : C1 à C164
    RCSlist = RCSlist + ['C'+str(i) for i in range(170)]
    # D - G.E.I.E. : D1 à D121
    RCSlist = RCSlist + ['D'+str(i) for i in range(130)]
    # E - Sociétés civiles : E1 à E7813
    RCSlist = RCSlist + ['E'+str(i) for i in range(8300)]
    # F - Associations sans but lucratif : F1 à F13236
    RCSlist = RCSlist + ['F'+str(i) for i in range(15000)]
    # G - Fondations : G1 à G253
    RCSlist = RCSlist + ['G'+str(i) for i in range(300)]
    # H - Association agricole : H1 à H141
    RCSlist = RCSlist + ['H'+str(i) for i in range(200)]
    # I - Association d'épargne-pension : I1 à I16
    RCSlist = RCSlist + ['I'+str(i) for i in range(20)]
    # J - Etablissements publics : J1 à J133
    RCSlist = RCSlist + ['J'+str(i) for i in range(200)]
    # K - Fonds commun de placement : K1 à K2117
    RCSlist = RCSlist + ['K'+str(i) for i in range(2500)]
    # M - Mutuelles : M1 à M4
    RCSlist = RCSlist + ['M'+str(i) for i in range(10)]
    return RCSlist