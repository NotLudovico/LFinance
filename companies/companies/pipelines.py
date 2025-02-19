import json


class MergeBSMS:
    def __init__(self):
        self.products = []

    def process_item(self, item, spider):

        found = False
        for i, company in enumerate(self.products):
            if company["isin"] == item["isin"]:
                self.products[i].update(item)
                found = True

        if not found:
            self.products.append(item)

        return item

    def close_spider(self, spider):
        # Read existing data from a JSONL file, handling the case where the file may not exist.
        try:
            with open("data_ms_bs.jsonl", "r", encoding="utf-8") as f:
                data = [json.loads(line) for line in f if line.strip()]
        except FileNotFoundError:
            data = []

        # Create a set of existing ISINs for fast duplicate checking.
        existing_isins = {entry["isin"] for entry in data}

        # Process new products and add those that don't already exist.
        for product in self.products:
            product_data = dict(product)
            if product_data["isin"] not in existing_isins:
                data.append(product_data)
                existing_isins.add(product_data["isin"])

        # Sort the data alphabetically by the 'name' key.
        data.sort(key=lambda entry: entry["name"])

        # Write the merged data back to the file as JSON Lines.
        with open("data_ms_bs.jsonl", "w", encoding="utf-8") as f:
            for entry in data:
                f.write(json.dumps(entry) + "\n")
