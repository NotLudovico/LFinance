import json
import httpx
import asyncio


class Vanguard:
    url = "https://www.it.vanguard/gpx/graphql"
    port_ids = [
        "9104",
        "9106",
        "9107",
        "9109",
        "9110",
        "9113",
        "9114",
        "9115",
        "9117",
        "9118",
        "9120",
        "9121",
        "9122",
        "9125",
        "9129",
        "9132",
        "9134",
        "9135",
        "9136",
        "9138",
        "9139",
        "9140",
        "9141",
        "9142",
        "9143",
        "9144",
        "9145",
        "9146",
        "9147",
        "9148",
        "9149",
        "9154",
        "9155",
        "9157",
        "9158",
        "9159",
        "9161",
        "9162",
        "9163",
        "9164",
        "9165",
        "9166",
        "9167",
        "9168",
        "9172",
        "9173",
        "9174",
        "9175",
        "9176",
        "9177",
        "9178",
        "9179",
        "9180",
        "9181",
        "9182",
        "9183",
        "9184",
        "9186",
        "9187",
        "9188",
        "9189",
        "9190",
        "9191",
        "9192",
        "9193",
        "9195",
        "9196",
        "9197",
        "9199",
        "9207",
        "9211",
        "9219",
        "9223",
        "9229",
        "9246",
        "9247",
        "9248",
        "9253",
        "9254",
        "9257",
        "9258",
        "9259",
        "9260",
        "9261",
        "9262",
        "9263",
        "9267",
        "9268",
        "9271",
        "9272",
        "9275",
        "9276",
        "9284",
        "9304",
        "9318",
        "9319",
        "9340",
        "9341",
        "9342",
        "9343",
        "9344",
        "9345",
        "9346",
        "9347",
        "9348",
        "9349",
        "9352",
        "9353",
        "9366",
        "9367",
        "9368",
        "9376",
        "9377",
        "9383",
        "9385",
        "9429",
        "9430",
        "9431",
        "9432",
        "9433",
        "9434",
        "9435",
        "9436",
        "9440",
        "9442",
        "9443",
        "9459",
        "9460",
        "9461",
        "9462",
        "9463",
        "9464",
        "9465",
        "9466",
        "9467",
        "9468",
        "9470",
        "9471",
        "9472",
        "9473",
        "9474",
        "9475",
        "9476",
        "9477",
        "9478",
        "9479",
        "9490",
        "9491",
        "9492",
        "9493",
        "9494",
        "9495",
        "9496",
        "9497",
        "9499",
        "9500",
        "9501",
        "9502",
        "9503",
        "9504",
        "9505",
        "9506",
        "9507",
        "9509",
        "9516",
        "9518",
        "9519",
        "9520",
        "9522",
        "9523",
        "9524",
        "9525",
        "9527",
        "9529",
        "9530",
        "9534",
        "9535",
        "9536",
        "9541",
        "9543",
        "9544",
        "9547",
        "9580",
        "9581",
        "9591",
        "9592",
        "9594",
        "9596",
        "9598",
        "9599",
        "9600",
        "9618",
        "9627",
        "9659",
        "9660",
        "9661",
        "9662",
        "9664",
        "9674",
        "9675",
        "9676",
        "9677",
        "9678",
        "9679",
        "9680",
        "9681",
        "9682",
        "9683",
        "9684",
        "9685",
        "9688",
        "9689",
        "9690",
        "9693",
        "9694",
        "9695",
        "9750",
        "9791",
        "9792",
        "9826",
        "9827",
        "9829",
        "9834",
        "9837",
        "9838",
        "9840",
        "9842",
        "9843",
        "9844",
        "9846",
        "9857",
        "9859",
        "9862",
        "9871",
        "9873",
        "9874",
        "9876",
        "9878",
        "9879",
        "9881",
        "9884",
        "9885",
        "9886",
        "9887",
        "9890",
        "9892",
        "9894",
        "9899",
        "9900",
        "9901",
        "9914",
        "9918",
        "9919",
        "9921",
        "9922",
        "9923",
        "9927",
        "9930",
        "9931",
        "9932",
        "9933",
        "9956",
        "9970",
        "9971",
        "9972",
        "9986",
        "9990",
        "9991",
        "E001",
        "E002",
        "E003",
        "E004",
        "E005",
        "E006",
        "E007",
        "E008",
        "E009",
        "E011",
        "E020",
        "E021",
        "E022",
        "E023",
        "E024",
        "E025",
        "E026",
        "E027",
        "E028",
        "E029",
        "E030",
        "E031",
        "E034",
        "E035",
        "E036",
        "E037",
        "E038",
        "E039",
        "E040",
        "E041",
        "E042",
        "E043",
        "E044",
        "E045",
        "E046",
        "E047",
        "E048",
        "E049",
        "E051",
        "E052",
        "E053",
        "E054",
        "E055",
        "E056",
        "E057",
        "E058",
        "E062",
        "E063",
        "E066",
        "E067",
        "E068",
        "E069",
        "E070",
        "E071",
        "E072",
        "E073",
        "E074",
        "E075",
        "E076",
        "E077",
        "E078",
        "E079",
        "E080",
        "E081",
        "E082",
    ]
    data = []
    has_geo = False
    has_general = False
    has_sector = False

    def __init__(self):
        self.url = "https://www.it.vanguard/gpx/graphql"
        self._client = httpx.AsyncClient(
            headers={"Content-Type": "application/json", "x-consumer-id": "GPX"},
            http2=True,
        )

    def merge(self, new_data, by="fullFundName"):
        if len(self.data) == 0:
            self.data = new_data
        else:
            index = {d[by]: d.copy() for d in self.data}

            for d in new_data:
                if by in d:
                    name = d[by]
                    if name in index:
                        index[name].update(d)
                    else:
                        index[name] = d.copy()

            self.data = list(index.values())

    async def geodata(self):
        if self.has_geo:
            return
        query = """
        query MarketAllocationGqlQuery($portIds: [String!]!) {
        funds(portIds: $portIds) {
            profile {
            fundFullName
            primaryMarketEquityClassification
            polarisPdtTypeIndicator
            marketOfDomicile
            }
            marketAllocation {
            portId
            date
            countryCode
            countryName
            fundMktPercent
            holdingStatCode
            benchmarkMktPercent
            regionCode
            regionName
            }
        }
        }
        """
        payload = {
            "query": query,
            "operationName": "MarketAllocationGqlQuery",
            "variables": {"portIds": self.port_ids},
        }
        resp = await self._client.post(self.url, json=payload)
        resp.raise_for_status()

        geo_data = resp.json()

        for fund in geo_data["data"]["funds"]:
            for k, v in fund["profile"].items():
                fund[k] = v
            del fund["profile"]

            if len(fund["marketAllocation"]) != 0:
                fund["portId"] = fund["marketAllocation"][0]["portId"]
                fund["date"] = fund["marketAllocation"][0]["date"]

                for mkt in fund["marketAllocation"]:
                    # del mkt["countryName"]
                    mkt["country"] = mkt["countryName"]
                    del mkt["countryName"]
                    del mkt["holdingStatCode"]
                    del mkt["regionName"]
                    del mkt["portId"]
                    del mkt["date"]

        self.has_geo = True
        self.merge(geo_data["data"]["funds"], "portId")
        return geo_data["data"]["funds"]

    async def general(self):
        if self.has_general:
            return
        query = """
        query FundPageHeadingQuery(
        $portIds: [String!]!,
        $businessLines: [String!]!,
        $countryCodes: [String!]!,
        $languages: [String!]!,
        $docTypes: [String!]!,
        $languageCodes: [String!]!,
        $portfolioLabelTypes: [String!]
        ) {
        funds(portIds: $portIds) {
            portId
            profile {
                portId
                fundCurrency
                fundFullName
                shareClassName
                marketOfDomicile
                distributionStrategy
                currencyHedgingStrategy
                fundGroupLifeStrategyFunds
                fundGroupTargetRetirementFunds
                identifiers {
                    altId
                    altIdCode
                    altIdValue
                }
                listings {
                    portId
                    exchange
                    fundCurrency
                    stockExchangeMarketIdentifierCode
                    identifiers(
                    altIds: [
                        "Bloomberg",
                        "Deutsche Boerse Ticker",
                        "NYSE Euronext Exchange Ticker",
                        "TIDM",
                        "Borsa Italiana Ticker",
                        "SIX Swiss Exchange Ticker",
                        "Bolsa Ticker",
                        "Ticker - Canada"
                    ]
                    ) {
                    altId
                    altIdValue
                    altIdCode
                    }
                }
                polarisPdtTypeIndicator
                assetClassificationLevel1
                relatedShareClassesPolaris {
                    currencyHedgingStrategy
                    distributionStrategy
                    fundCurrency
                    fundFullName
                    marketOfDomicile
                    portId
                    shareClassName
                }
            }
            portfolioLabels(languageCodes: $languageCodes, names: $portfolioLabelTypes) {
            labels {
                name
                langCode
                value
            }
            }
            documentDetails(
            businessLines: $businessLines
            countryCodes: $countryCodes
            languages: $languages
            docTypes: $docTypes
            ) {
            countryCode
            countryCodes
            businessLines
            languageCode
            name
            path
            publishDate
            type
            }
        }
        }
        """

        variables = {
            "portIds": self.port_ids,
            "businessLines": ["TINS"],
            "countryCodes": ["IT"],
            "languages": ["IT", "EN"],
            "docTypes": ["PS", "ME", "AR", "LIR", "FS", "PRP", "SFDR"],
            "languageCodes": ["en"],
            "portfolioLabelTypes": ["fundName"],
        }

        payload = {
            "operationName": "FundPageHeadingQuery",
            "query": query,
            "variables": variables,
        }
        resp = await self._client.post(self.url, json=payload)
        resp.raise_for_status()

        data = resp.json()

        for fund in data["data"]["funds"]:
            del fund["portfolioLabels"]
            del fund["documentDetails"]
            del fund["portId"]
            for k, v in fund["profile"].items():
                fund[k] = v
            del fund["profile"]

            for ident in fund["identifiers"]:
                if ident["altIdCode"] == "BBHT":
                    continue
                fund[ident["altIdCode"]] = ident["altIdValue"]
            del fund["identifiers"]
            del fund["relatedShareClassesPolaris"]

        self.has_general = True
        self.merge(data["data"]["funds"], by="portId")
        return data["data"]["funds"]

    async def sectors(self):
        if self.has_sector:
            return
        payload = {
            "operationName": "getSectorDiversification",
            "variables": {"portIds": self.port_ids},
            "query": """
                    query getSectorDiversification($portIds: [String!]!) {
                        funds(portIds: $portIds) {
                            profile {
                                portId
                                fundFullName
                                primarySectorEquityClassification
                            }
                            sectorDiversification {
                                sectorName
                                fundPercent
                                benchmarkPercent
                            }
                        }
                    }
                """,
        }
        resp = await self._client.post(self.url, json=payload)
        resp.raise_for_status()

        data = resp.json()
        missing_ids = []
        for fund in data["data"]["funds"]:
            for k, v in fund["profile"].items():
                fund[k] = v
            del fund["profile"]
            fund["sectors"] = fund["sectorDiversification"]
            del fund["sectorDiversification"]

            if len(fund["sectors"]) == 0:
                missing_ids.append(fund["portId"])

        self.has_sector = True
        self.merge(data["data"]["funds"], by="portId")
        missing = await self.bond_sector(missing_ids)
        self.merge(missing, by="portId")
        return data["data"]["funds"]

    async def bond_sector(self, port_ids):
        payload = {
            "operationName": "CreditIssuerQuery",
            "variables": {"portIds": port_ids},
            "query": """
                query CreditIssuerQuery($portIds: [String!]!) { 
                    funds(portIds: $portIds) {
                        profile { 
                            marketOfDomicile 
                        } 
                    } 
                    issuerHistory(portIds: $portIds) {
                        portId 
                        issuer(limit: -1, getLatest: true) { 
                            compositions { 
                                sectorName 
                                subSectorName 
                                value 
                                benchmarkValue 
                            } 
                        }
                    } 
                }
            """,
        }
        resp = await self._client.post(self.url, json=payload)
        resp.raise_for_status()  # raises if 4xx/5xx
        data = resp.json()  # only reached on success

        for fund in data["data"]["issuerHistory"]:
            fund["sectors"] = fund["issuer"][0]["compositions"]
            del fund["issuer"]
        del data["data"]["funds"]
        return data["data"]["issuerHistory"]

    async def fees(self):
        payload = {
            "operationName": "FeesAndExpensesQuery",
            "variables": {"portIds": self.port_ids},
            "query": """
                query FeesAndExpensesQuery($portIds: [String!]!) {
                    funds(portIds: $portIds) {
                        portId
                        profile {
                            feesAndExpenses(getLatest: true) {
                                feesAndExpensesType {
                                    expenseType(codes: ["TOTEXPRTPC", "ADJEXPRTPC"]) {
                                        code
                                        eipcode
                                        value
                                        startDate
                                    }
                                }
                            }
                        }
                    }
                }
            """,
        }

        resp = await self._client.post(self.url, json=payload)
        resp.raise_for_status()
        data = resp.json()

        for fund in data["data"]["funds"]:
            for k, v in fund["profile"].items():
                fund[k] = v
            del fund["profile"]
        self.merge(data["data"]["funds"], by="portId")
        return data["data"]["funds"]

    def to_file(self, name):
        file = open(name, "w+")
        file.write(json.dumps({"data": self.data}, indent=2))
        file.close()


async def main():
    vgn = Vanguard()
    await asyncio.gather(vgn.general(), vgn.geodata(), vgn.sectors(), vgn.fees())
    vgn.to_file("vanguard.json")
    await vgn._client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
