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
        data = []

        # Process new products and add those that don't already exist.
        for product in self.products:
            print(product)
            product_data = dict(product)
            data.append(product_data)

        # Write the merged data back to the file as JSON Lines.
        with open("data_ms_bs.jsonl", "w", encoding="utf-8") as f:
            for entry in data:
                f.write(json.dumps(entry) + "\n")
