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
        with open("data_ms_bs.jsonl", "w", encoding="utf-8") as f:
            for product in self.products:
                f.write(json.dumps(dict(product)) + "\n")
