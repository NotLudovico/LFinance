ETF_PROPERTY_TRANSLATE = {
    "Globale": "global",
    "Nord America": "north america",
    "Europa": "europe",
    "Medio Oriente e Africa": "middle east and africa",
    "Asia Pacifico": "asia pacific",
    "Azionario": "equity",
    "Reddito Fisso": "fixed income",
    "Immobiliare": "real estate",
    "IT": "information technology",
    "Finanziari": "financials",
    "Materiali": "materials",
    "Imprese di servizi di pubblica utilità": "public utility companies",
    "Comunicazione": "communications",
    "Industriali": "industrials",
    "Consumi discrezionali": "discretionary spending",
    "Generi di largo consumo": "consumer goods",
    "Salute": "health",
    "Tesoro": "treasury",
    "Energia": "energy",
    "Immobili": "real estate",
    "Attività bancarie": "banking",
    "Distribution": "dist",
    "UCITS ETF EUR Dist": "dist",
    "Accumulation": "acc",
    "EUR Acc": "acc",
    "EUR Dist": "dist",
    "UCITS ETF Acc": "acc",
    "UCITS ETF EUR Acc": "acc",
    "UCITS ETF DR (C)": "acc",
    "UCITS ETF Acc EUR Hedged": "acc",
    "UCITS ETF Dist": "dist",
    "UCITS ETF DR - USD (D)": "dist",
    "EUR Hedged Dist": "dist",
    "UCITS ETF EUR Hedged Dist": "dist",
    "UCITS ETF USD Hedged Dist": "dist",
    "Direct(Physical)": "physical",
    "Indirect(Swap Based)": "swap",
}


def translate(value):
    if value and value in ETF_PROPERTY_TRANSLATE:
        return ETF_PROPERTY_TRANSLATE[value]
    if value:
        return value.lower()
    else:
        return None
