ETF_PROPERTY_TRANSLATE = {
    "Globale": "global",
    "Nord America": "north america",
    "Europa": "europe",
    "Medio Oriente e Africa": "middle east and africa",
    "Asia Pacifico": "asia pacific",
    "Azionario": "equity",
    "Reddito Fisso": "fixed income",
    "Immobiliare": "real estate",
}


def translate(value):
    if value and value in ETF_PROPERTY_TRANSLATE:
        return ETF_PROPERTY_TRANSLATE[value]
    else:
        return value.lower()
