import json


def load_msbs_data(msbs_filename):
    """
    Load data from data_ms_bs.jsonl and return a dictionary mapping ISIN to the record.
    """
    msbs_data = {}
    with open(msbs_filename, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                isin = record.get("isin")
                if isin:
                    msbs_data[isin] = record
            except json.JSONDecodeError as err:
                print(f"Error parsing line in {msbs_filename}: {err}")
    return msbs_data


def update_companies(companies_filename, msbs_data, output_filename):
    """
    Process companies.jsonl. For each company with a missing market_cap,
    look up the corresponding ISIN in the msbs_data and update market_cap if found.
    The updated companies are written to output_filename.
    """
    with open(companies_filename, "r", encoding="utf-8") as fin, open(
        output_filename, "w", encoding="utf-8"
    ) as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                company = json.loads(line)
            except json.JSONDecodeError as err:
                print(f"Error parsing company record: {err}")
                continue
            features = ["market_cap", "sector", "sub_sector", "p_e"]
            for f in features:
                # Check if market_cap is missing or None
                if f not in company or company[f] is None:
                    isin = company.get("isin")
                    if isin and isin in msbs_data:
                        ms_record = msbs_data[isin]
                        # Update only if ms_record has a valid market_cap
                        if f in ms_record and ms_record[f] is not None:
                            company[f] = ms_record[f]

            # Write the (possibly updated) company record back to output file
            fout.write(json.dumps(company) + "\n")


if __name__ == "__main__":
    companies_file = "companies.jsonl"
    msbs_file = "data_ms_bs.jsonl"
    output_file = "companies_updated.jsonl"

    # Load market cap data from data_ms_bs.jsonl
    msbs_data = load_msbs_data(msbs_file)

    # Update companies data and write to a new file
    update_companies(companies_file, msbs_data, output_file)

    print(f"Updated companies data has been written to {output_file}")
