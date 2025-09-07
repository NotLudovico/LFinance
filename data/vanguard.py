import json
from utilities.common import Holding, ETF
from utilities.database import (
    open_db,
    upsert_holding,
    upsert_security,
    upsert_etf,
    setup_database,
)

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


import requests


def get_etf_list():
    url = "https://www.it.vanguard/gpx/graphql"

    payload = {
        "operationName": "FundsQuery",
        "variables": {
            "portIds": port_ids,
            "businessLine": "TRTL",
            "countryCode": "IT",
            "languages": ["EN", "IT"],
            "countryCodes": ["ITALY"],
            "skipSummaryReturns": True,
            "skipQuarterlyReturns": False,
            "skipCumulativeReturns": False,
            "skipRisk": False,
            "getLatest": True,
            "documentTypes": ["PRP", "PRA", "PS", "ME", "AR", "LIR"],
            "languageCodes": ["en"],
            "portfolioLabelTypes": ["fundName"],
        },
        "query": """query FundsQuery($portIds: [String!]!, $businessLine: String!, $countryCode: String!, $languages: [String!]!, $countryCodes: [COUNTRY_CODES!], $skipSummaryReturns: Boolean!, $skipQuarterlyReturns: Boolean!, $skipCumulativeReturns: Boolean!, $skipRisk: Boolean!, $getLatest: Boolean!, $documentTypes: [String!]!, $languageCodes: [String!]!, $portfolioLabelTypes: [String!]!) {
      borTotalHoldings(portIds: $portIds) {
        portId
        totalHoldings
        totalDelayeredHoldings
        totalMoneyMarketHoldings
     __typename
      }
      funds(portIds: $portIds) {
        portId
        portfolioLabels(
          countryCodes: $countryCodes
          languageCodes: $languageCodes
          names: $portfolioLabelTypes
        ) {
          labels {
            name
            value
            __typename
          }
          __typename
        }
        documentDetails(
          businessLines: [$businessLine]
          countryCodes: [$countryCode]
          languages: $languages
          docTypes: $documentTypes
        ) {
          languageCode
          languageCodes
          name
          path
          publishDate
          type
          countryCode
          countryCodes
          businessLine
          businessLines
          __typename
        }
        distributionDetails {
          periodicDistributions(limit: 1) {
            items {
              exDividendDate
              recordDate
              payableDate
              reinvestPrice
              reinvestmentDate
              scheduleType {
                scheduleCode
                scheduleDesc
                __typename
              }
              taxDetails {
                distributionAmount
                distributionType {
                  distCode
                  distDesc
                  __typename
                }
                currencyCode
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        performanceDetails {
          items {
            rollingReturns(limit: 10) {
              fund {
                asOfDate
                return_pct
                __typename
              }
              __typename
            }
            cumulativeReturns @skip(if: $skipCumulativeReturns) {
              totalReturns(
                limit: 1
                returnPeriodCodes: [SINCE_CALENDAR_YEAR, ONE_YEAR, THREE_YEAR, FIVE_YEAR, TEN_YEAR, SINCE_INCEPTION]
              ) {
                items {
                  portId
                  returnPeriod
                  percent
                  effectiveDate
                  returnType
                  __typename
                }
                __typename
              }
              __typename
            }
            summaryReturns @skip(if: $skipSummaryReturns) {
              totalReturns(getLatest: $getLatest) {
                items {
                  portId
                  returnPeriod
                  percent
                  effectiveDate
                  returnType
                  __typename
                }
                __typename
              }
              marketReturns(getLatest: $getLatest) {
                items {
                  portId
                  returnPeriod
                  percent
                  effectiveDate
                  returnType
                  __typename
                }
                __typename
              }
              benchmarkReturns(getLatest: $getLatest) {
                items {
                  effectiveDate
                  portId
                  retPercent
                  returnPeriodCode
                  __typename
                }
                __typename
              }
              __typename
            }
            quarterlyReturns @skip(if: $skipQuarterlyReturns) {
              totalReturns(limit: -1, returnPeriodCodes: [ONE_YEAR], sortAsc: true) {
                items {
                  portId
                  effectiveDate
                  percent
                  returnPeriod
                  returnType
                  returnClass
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        pricingDetails {
          navPrices(limit: 1) {
            items {
              measureTypeCode
              asOfDate
              currencyCode
              price
              amountChange
              percentChange
              __typename
            }
            __typename
          }
          marketPrices(limit: 1) {
            items {
              portId
              items {
                measureTypeCode
                asOfDate
                currencyCode
                price
                amountChange
                percentChange
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        profile {
          portId
          polarisPdtTypeIndicator
          fundIndicator
          assetClassificationLevel1
          productTypeLevel1
          marketOfDomicile
          consarApproved
          fundGroupHedgedFunds
          fundFullName
          prospectusShareClassName
          fundInceptionDate
          currencyHedgingStrategy
          closedToAllPurchases
          distributionStrategy
          fundCurrency
          fundGroupLifeStrategyFunds
          fundGroupTargetRetirementFunds
          shareClassName
          managementStrategy
          marketRegionFocus
          countryMarketedForSale
          assetClassSubcategories {
            INT {
              level3
              __typename
            }
            __typename
          }
          listings {
            portId
            fundCurrency
            exchange
            stockExchangeMarketIdentifierCode
            fundInceptionDate
            identifiers(
              altIds: [\"Bloomberg\", \"RIC\", \"SEDOL\", \"Bloomberg iNAV\", \"Deutsche Boerse Ticker\", \"NYSE Euronext Exchange Ticker\", \"TIDM\", \"Borsa Italiana Ticker\", \"SIX Swiss Exchange Ticker\", \"Bolsa Ticker\", \"Ticker\", \"Ticker - Canada\"]
            ) {
              altId
              altIdValue
              altIdCode
              __typename
            }
            __typename
          }
          identifiers(
            altIds: [\"ISIN\", \"CITI Code\", \"CUSIP\", \"MexId\", \"Bloomberg\", \"SEDOL\", \"WKN Code\", \"VALOREN - Swiss Security Number\", \"Ticker\", \"Bolsa Ticker\", \"Ticker - Canada\", \"FundServ Code\"]
          ) {
            altId
            altIdCode
            altIdValue
            __typename
          }
          feesAndExpenses(getLatest: $getLatest) {
            feesAndExpensesType {
              expenseType(codes: [\"TOTEXPRTPC\", \"ADJEXPRTPC\"]) {
                code
                value
                __typename
              }
              feeType(codes: \"MFEXP\") {
                value
                code
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      polarisAnalyticsHistory(portIds: $portIds) {
        portId
        daily {
          yields {
            fund {
              items {
                codes {
                  NETYLD {
                    asOfDate: effectiveDate
                    statValue: percent
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          volatility @skip(if: $skipRisk) {
            fund(getLatest: true) {
              items {
                codes {
                  SRRI17WK {
                    volatilityValue
                    effectiveDate
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        monthly {
          yields {
            fund(getLatest: true) {
              items {
                codes {
                  YLDDISTUK {
                    asOfDate: effectiveDate
                    statValue: percent
                    __typename
                  }
                  YLDHIST {
                    asOfDate: effectiveDate
                    statValue: percent
                    __typename
                  }
                  FRCDIVYLD {
                    asOfDate: effectiveDate
                    statValue: percent
                    __typename
                  }
                  YLDT12DIV {
                    asOfDate: effectiveDate
                    statValue: percent
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          valuation {
            fund(limit: 5) {
              items {
                AUM {
                  fundAssetAmount
                  currency
                  effectiveDate
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          analytics {
            portfolioStatistics(getLatest: true) {
              items {
                codes {
                  CSTOCK {
                    value
                    effectiveDate
                    __typename
                  }
                  NBONDS {
                    value
                    effectiveDate
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
    }
    """,
    }
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7,it;q=0.6",
        "apollographql-client-name": "gpx",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "dnt": "1",
        "origin": "https://www.it.vanguard",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "x-consumer-id": "it0",
    }

    response = requests.request("POST", url, json=payload, headers=headers)

    funds = json.loads(response.text)["data"]["funds"]
    pid_isin = {}
    for fund in funds:
        print(fund["portId"])
        try:
            pid_isin[fund["portId"]] = [
                x for x in fund["profile"]["identifiers"] if x["altId"] == "ISIN"
            ][0]["altIdValue"]
        except:
            pid_isin[fund["portId"]] = None

    return pid_isin


def get_holdings_data(pid, isin, last_item_key=None):
    url = "https://www.it.vanguard/gpx/graphql"

    payload = {
        "operationName": "FundsHoldingsQuery",
        "variables": {
            "portIds": [pid],
            "lastItemKey": last_item_key,
            "securityTypes": [
                "FI.ABS",
                "FI.CONV",
                "FI.CORP",
                "FI.IP",
                "FI.LOAN",
                "FI.MBS",
                "FI.MUNI",
                "FI.NONUS_GOV",
                "FI.US_GOV",
                "MM.AGC",
                "MM.BACC",
                "MM.CD",
                "MM.CP",
                "MM.MCP",
                "MM.RE",
                "MM.TBILL",
                "MM.TD",
                "MM.TFN",
                "EQ.DRCPT",
                "EQ.ETF",
                "EQ.FSH",
                "EQ.PREF",
                "EQ.PSH",
                "EQ.REIT",
                "EQ.STOCK",
                "EQ.RIGHT",
                "EQ.WRT",
                "MF.MF",
            ],
        },
        "query": "query FundsHoldingsQuery($portIds: [String!], $securityTypes: [String!], $lastItemKey: String) { funds(portIds: $portIds) { profile { fundFullName fundCurrency primarySectorEquityClassification __typename } __typename } borHoldings(portIds: $portIds) { holdings(limit: 1500, securityTypes: $securityTypes, lastItemKey: $lastItemKey) { items { issuerName securityLongDescription gicsSectorDescription icbSectorDescription icbIndustryDescription marketValuePercentage sedol1 quantity ticker securityType finalMaturity effectiveDate marketValueBaseCurrency bloombergIsoCountry couponRate isin  } totalHoldings lastItemKey  }  } }",
    }
    headers = {"content-type": "application/json", "x-consumer-id": "GPX"}

    response = requests.request("POST", url, json=payload, headers=headers)

    holdings = json.loads(response.text)["data"]["borHoldings"][0]["holdings"]["items"]

    last_key = json.loads(response.text)["data"]["borHoldings"][0]["holdings"][
        "lastItemKey"
    ]

    cleaned = []
    for holding in holdings:
        cleaned.append(
            Holding(
                etf_isin=isin,
                holding_isin=holding["isin"],
                weight=holding["marketValuePercentage"],
                holding_name=holding["issuerName"],
                sector=holding["icbIndustryDescription"],
                country=holding["bloombergIsoCountry"],
                currency=None,
            )
        )

    if last_key is not None:
        cleaned.extend(get_holdings_data(pid, isin, last_item_key=last_key))

    return cleaned


if __name__ == "__main__":
    pid_to_isin = get_etf_list()

    # keep only valid ISINs
    isins_to_update = [i for i in pid_to_isin.values() if i and len(i) == 12]

    with open_db("database.db") as conn:
        setup_database(conn)
        # 1) ensure ETFs exist (minimal facts)
        for isin in isins_to_update:
            upsert_etf(conn, ETF(isin=isin, issuer="vanguard").to_db_tuple())

        # 2) clear old holdings for these ETFs (avoid stale rows)
        if isins_to_update:
            placeholders = ", ".join("?" for _ in isins_to_update)
            conn.execute(
                f"DELETE FROM etf_holdings WHERE etf_isin IN ({placeholders})",
                isins_to_update,
            )

        # 3) fetch + upsert securities & holdings
        for pid, etf_isin in pid_to_isin.items():
            if not etf_isin or len(etf_isin) != 12:
                continue

            try:
                rows = get_holdings_data(pid, etf_isin)  # -> List[Holding]
            except Exception as e:
                print(f"Skipping {pid} ({etf_isin}) due to error: {e}")
                continue

            for h in rows:
                # basic hygiene like other scrapers
                if h.holding_isin and len(h.holding_isin) != 12:
                    continue
                weight = h.weight if (h.weight is not None and h.weight >= 0) else 0.0
                name = h.holding_name or (
                    "CASH" if h.sector == "cash" else h.holding_isin or "UNKNOWN"
                )

                sec_id = upsert_security(
                    conn,
                    isin=h.holding_isin,
                    name=str(name),
                    sector=h.sector,
                    country=h.country,
                    currency=h.currency,
                )
                upsert_holding(
                    conn, etf_isin=etf_isin, security_id=sec_id, weight=weight
                )

    print("✅ Vanguard holdings saved to database.")
