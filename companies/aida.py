import jsonlines
import re
import logging
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from dotenv import load_dotenv
import os

load_dotenv("local.env")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration
CONFIG = {
    "LOGIN_URL": "https://login.bvdinfo.com/R1/AidaNeo",
    "HOME_URL": "https://aida-r1.bvdinfo.com/",
    "DETAIL_PAGE_URL": "https://aida-r1.bvdinfo.com/Report.serv?product=aidaneo&RecordInternalId={}",
    "USERNAME": os.getenv("AIDA_USERNAME"),
    "PASSWORD": os.getenv("AIDA_PASSWORD"),
    "HEADLESS": False,
}


def into_num(num: str) -> float:
    num = num.replace(".", "").replace(",", ".")
    try:
        return int(num)
    except ValueError:
        return round(float(num), 2)


def load_previous_data(file_path="test_final_f.jsonl"):
    prev_data = []
    try:
        with jsonlines.open(file_path) as reader:
            for company in reader:
                prev_data.append(company)
    except FileNotFoundError:
        logging.warning(f"{file_path} not found. Creating a new file.")
    return prev_data


def login(page):
    try:
        logging.info("Logging in...")
        page.goto(CONFIG["LOGIN_URL"], timeout=60000)
        page.locator("#user").fill(CONFIG["USERNAME"])
        page.locator("#pw").fill(CONFIG["PASSWORD"])
        page.locator("#bnLoginNeo").click()
        page.locator("//*[@id='loginEnter']/table/tbody/tr[2]/td/input").click()
    except PlaywrightTimeoutError:
        logging.error("Login page load timeout. Exiting...")
        raise SystemExit("Login failed due to timeout.")


def process_company(company, page, writer):
    if "name" not in company:
        logging.error("Company entry missing 'name' key. Skipping...")
        return
    company_name = company["name"]
    logging.info(f"Processing {company_name}...")

    if company["isin"][:2] != "IT":
        logging.info(f"{company_name} is not an Italian company. Skipping...")
        writer.write(company)
        return

    try:
        page.goto(CONFIG["HOME_URL"], wait_until="load", timeout=60000)
        search_box = "input[name='ContentContainer1$ctl00$Header$ctl00$ctl00$ctl03$SearchText2008']"
        page.wait_for_selector(search_box, timeout=10000)
        page.locator(search_box).fill(
            company["name"].replace("amp;", "").replace("'", " ")
        )
        page.keyboard.press("Enter")

        logging.info("Listing results...")
        page.wait_for_load_state("load", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
    except PlaywrightTimeoutError:
        logging.error(f"Timeout while searching for {company_name}. Skipping...")
        return

    if page.title() == "Aida - Home":
        logging.warning(f"{company_name} not found. Skipping...")
        writer.write(company)
        return

    # Check if company is publicly traded
    if "Aida - List" in page.title():
        public_traded = page.locator("a.nameGreen")
        if public_traded.count() != 0:
            page.goto(
                CONFIG["HOME_URL"]
                + re.search(
                    r"'(Report\.serv\?.*?)'",
                    public_traded.first.get_attribute("onclick"),
                ).group(1),
                wait_until="load",
                timeout=60000,
            )
        else:
            logging.info(f"{company_name} is not publicly traded. Skipping...")
            writer.write(company)
            return

    try:
        extract_financial_data(company, page)
        if "ebitda" in company:
            writer.write(company)
            logging.info(f"✅ Successfully saved data for {company_name}")
        else:
            logging.warning(
                f"⚠️ No financial data found for {company_name}, skipping write."
            )
    except PlaywrightTimeoutError:
        logging.error(
            f"Timeout while extracting financial data for {company_name}. Skipping..."
        )
        return


def extract_financial_data(company, page):
    try:
        page.locator(
            "//*[@id='m_ContentControl_ContentContainer1_ctl00_RightMenus_SectionsMenu_MenuControl']/li[7]"
        ).click()
        page.locator(
            "//*[@id='m_ContentControl_ContentContainer1_ctl00_RightMenus_SectionsMenu_MenuControl']/li[7]/ul/li[5]"
        ).click()

        href = page.locator("//a[text()='Display consolidated accounts']")
        if href.count() != 0:
            href = href.first.get_attribute("href")
            id_match = re.search(r"(\d+)", href)
            if id_match:
                page.goto(
                    CONFIG["DETAIL_PAGE_URL"].format(id_match.group(1)), timeout=60000
                )

        multiplier = 1000 if "th" in page.locator("td.fmh").first.inner_text() else 1
        categories = [
            (4, "revenues", multiplier),
            (5, "ebitda", multiplier),
            (6, "profit", multiplier),
            (7, "assets", multiplier),
            (8, "sh_funds", multiplier),
            (9, "net_fin", multiplier),
            (11, "ebitda_sales", 1),
            (12, "ros", 1),
            (13, "roa", 1),
            (14, "roe", 1),
            (15, "debt_equity", 1),
            (17, "debt_ebitda", 1),
            (20, "employees", 1),
        ]
        for row, key, factor in categories:
            for item in page.locator(
                f"//*[@id='m_ContentControl_ContentContainer1_ctl00_Content_Section_FINANCIAL_PROFILE_SSCtr']/tbody/tr[{row}]/td[@class='ft WHR WVT']"
            ).all():
                value = item.inner_text()
                if value not in ["n.a.", "n.s.", "n.d.", "nd"]:
                    value = into_num(value) * factor
                company.setdefault(key, []).append(value)
    except PlaywrightTimeoutError:
        logging.error(
            f"Timeout while navigating financial details for {company['name']}. Skipping..."
        )
        return


def main():
    prev_data = load_previous_data()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=CONFIG["HEADLESS"])
        context = browser.new_context(
            viewport={"width": 1440, "height": 1000}, locale="en-US"
        )
        page = context.new_page()
        login(page)

        with jsonlines.open("test_final_f.jsonl", "a") as writer:
            with jsonlines.open("data_ms_bs.jsonl") as reader:
                for company in reader:
                    if any(prev["isin"] == company["isin"] for prev in prev_data):
                        continue
                    process_company(company, page, writer)

        browser.close()
        logging.info("Finished processing all companies.")


if __name__ == "__main__":
    main()
