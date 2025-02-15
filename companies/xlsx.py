import jsonlines
import json
import xlsxwriter

workbook = xlsxwriter.Workbook("dati.xlsx")
worksheet = workbook.add_worksheet()


titles = [
    ("name", 1, "Name"),
    ("isin", 1, "Isin"),
    ("ticker", 1, "Ticker"),
    ("P/E", 1, "P/E"),
    ("yield", 1, "Yield (%)"),
    ("super_sector", 1, "Sector"),
    ("maket_cap", 1, "Market Cap."),
    ("1MP", 1, "1M (%)"),
    ("6MP", 1, "6M (%)"),
    ("1YP", 1, "1Y (%)"),
    ("sh_funds", 6, "Shareholder's Funds", "#85c7f2"),
    ("net_fin", 6, "Net Financial Position", "#a9e5bb"),
    ("ebitda", 6, "EBITDA", "#faf884"),
    ("ebitda_sales", 6, "EBITDA/sales (%)", "#ebcfb2"),
    ("debt_ebitda", 6, "Debt/EBITDA", "#ce84ad"),
    ("tang_assets", 6, "Total Tangible Assets", "#28536b"),
]

i = 0

for title in titles:
    if title[1] == 1:
        worksheet.write(
            0,
            i,
            title[2],
            workbook.add_format(
                {
                    "bold": 1,
                    "align": "center",
                    "right": 1,
                }
            ),
        )
    else:
        worksheet.merge_range(
            0,
            i,
            0,
            i + title[1] - 1,
            title[2],
            workbook.add_format(
                {"bold": 1, "align": "center", "right": 1, "fg_color": title[3]}
            ),
        )
        i += title[1] - 1
    i += 1


worksheet.freeze_panes(1, 1)

# Pull in data
with jsonlines.open("final.jsonl") as data:
    row = 1
    bg_format1 = workbook.add_format(
        {"bg_color": "#78B0DE"}
    )  # blue cell background color
    bg_format2 = workbook.add_format(
        {"bg_color": "#FFFFFF"}
    )  # white cell background color

    for company in data:
        cf = workbook.add_format({})
        cf.set_bg_color("#b5d1ff") if row % 2 == 0 else cf.set_bg_color("#ffffff")
        cf.set_right(1)

        i = 0
        for title in titles:
            if title[1] == 1:
                worksheet.write(
                    row,
                    i,
                    company[title[0]] if title[0] in company else "",
                    workbook.add_format(
                        {
                            "bg_color": "#b5d1ff" if row % 2 == 0 else "#ffffff",
                            "right": 1,
                            "font_color": (
                                "#000000"
                                if (
                                    (
                                        title[0] != "1MP"
                                        and title[0] != "6MP"
                                        and title[0] != "1YP"
                                    )
                                    or (title[0] not in company)
                                )
                                else "FF0000" if company[title[0]] < 0 else "00B050"
                            ),
                        }
                    ),
                )
                i += 1
            elif title[0] in company:
                for j in range(0, min(6, len(company[title[0]]))):
                    worksheet.write(
                        row,
                        i,
                        company[title[0]][j] if title[0] in company else "",
                        (
                            cf
                            if j == 5
                            else (
                                workbook.add_format(
                                    {
                                        "bg_color": (
                                            "#b5d1ff" if row % 2 == 0 else "#ffffff"
                                        )
                                    }
                                )
                            )
                        ),
                    )
                    i += 1
                for index in range(j + 1, 6):
                    if index == 5:
                        worksheet.write(row, i, "", cf)
                    else:
                        worksheet.write(
                            row,
                            i,
                            "",
                            workbook.add_format(
                                {"bg_color": ("#b5d1ff" if row % 2 == 0 else "#ffffff")}
                            ),
                        )
                    i += 1

        for index in range(i, 46):
            worksheet.write(
                row,
                index,
                "",
                workbook.add_format(
                    {
                        "bg_color": ("#b5d1ff" if row % 2 == 0 else "#ffffff"),
                        "right": 1 if index - 9 != 0 and (index - 9) % 6 == 0 else 0,
                    }
                ),
            )
        row += 1


workbook.close()
