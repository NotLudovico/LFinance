import jsonlines
import re
import logging
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
import os

load_dotenv("local.env")

# Configure logging with ANSI escape codes for bold text
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ANSI escape code for bold text
BOLD = "\033[1m"
RESET = "\033[0m"

# Configuration
CONFIG = {
    "LOGIN_URL": "https://login.bvdinfo.com/R1/AidaNeo",
    "HOME_URL": "https://aida-r1.bvdinfo.com/",
    "DETAIL_PAGE_URL": "https://aida-r1.bvdinfo.com/Report.serv?product=aidaneo&RecordInternalId={}",
    "USERNAME": os.getenv("AIDA_USERNAME"),
    "PASSWORD": os.getenv("AIDA_PASSWORD"),
    "HEADLESS": True,
}


# Convert number strings with European formatting to float/int
def into_num(num: str) -> float:
    num = num.replace(".", "").replace(",", ".")
    try:
        return int(num)
    except ValueError:
        return round(float(num), 2)


# Load previously processed data
def load_previous_data(file_path="test_final_f.jsonl"):
    prev_data = []
    try:
        with jsonlines.open(file_path) as reader:
            for company in reader:
                prev_data.append(company)
    except FileNotFoundError:
        logging.warning(f"{file_path} not found. Creating a new file.")
    return prev_data


# Login function
def login(page):
    logging.info("Logging in...")
    page.goto(CONFIG["LOGIN_URL"])
    page.locator("#user").fill(CONFIG["USERNAME"])
    page.locator("#pw").fill(CONFIG["PASSWORD"])
    page.locator("#bnLoginNeo").click()
    page.locator("//*[@id='loginEnter']/table/tbody/tr[2]/td/input").click()


# Process company search and extraction
def process_company(company, page, writer):
    company_name_bold = f"{BOLD}{company['name']}{RESET}"
    logging.info(f"Processing {company_name_bold}...")

    # Skip if already processed
    if company["isin"][:2] != "IT":
        logging.info(f"{company_name_bold} is not an Italian company. Skipping...")
        writer.write(company)
        return

    page.goto(CONFIG["HOME_URL"], wait_until="load")

    # Search for company
    search_box = (
        "input[name='ContentContainer1$ctl00$Header$ctl00$ctl00$ctl03$SearchText2008']"
    )
    page.wait_for_selector(search_box)
    page.locator(search_box).fill(
        company["name"]
        .replace("SpA", "")
        .replace("amp;", "")
        .replace("'", " ")
        .replace("S.p.A", "")
    )
    page.keyboard.press("Enter")

    logging.info("Listing results...")
    page.wait_for_load_state("load")
    page.wait_for_load_state("networkidle", timeout=60000)

    if page.title() == "Aida - Home":
        logging.warning(f"{company_name_bold} not found. Skipping...")
        writer.write(company)
        writer.flush()
        return

    # Check if company is publicly traded
    if "Aida - List" in page.title():
        public_traded = page.locator("a.nameGreen")
        if public_traded.count() != 0:
            logging.info(
                f"{company_name_bold} is publicly traded. Accessing details..."
            )
            page.goto(
                CONFIG["HOME_URL"]
                + re.search(
                    r"'(Report\.serv\?.*?)'",
                    public_traded.first.get_attribute("onclick"),
                ).group(1),
                wait_until="load",
            )
        else:
            logging.info(f"{company_name_bold} is not publicly traded. Skipping...")
            writer.write(company)
            writer.flush()
            return

    # Navigate to financial details
    logging.info(f"Extracting financial details for {company_name_bold}...")
    extract_financial_data(company, page)

    # Ensure company has data before writing
    if "ebitda" in company:
        writer.write(company)
        logging.info(f"✅ Successfully saved data for {company_name_bold}")
    else:
        logging.warning(
            f"⚠️ No financial data found for {company_name_bold}, skipping write."
        )


# Extract financial data
def extract_financial_data(company, page):
    # Click financial report section
    page.locator(
        "//*[@id='m_ContentControl_ContentContainer1_ctl00_RightMenus_SectionsMenu_MenuControl']/li[7]"
    ).click()
    page.locator(
        "//*[@id='m_ContentControl_ContentContainer1_ctl00_RightMenus_SectionsMenu_MenuControl']/li[7]/ul/li[5]"
    ).click()

    # Handle consolidated accounts link
    href = page.locator("//a[text()='Display consolidated accounts']")
    if href.count() != 0:
        href = href.first.get_attribute("href")
        id_match = re.search(r"(\d+)", href)
        if id_match:
            page.goto(CONFIG["DETAIL_PAGE_URL"].format(id_match.group(1)))

    # Determine multiplier
    multiplier = 1000 if "th" in page.locator("td.fmh").first.inner_text() else 1

    # Financial data categories
    categories = [
        (5, "ebitda", multiplier),
        (8, "sh_funds", multiplier),
        (9, "net_fin", multiplier),
        (11, "ebitda_sales", 1),
        (17, "debt_ebitda", 1),
    ]

    for row, key, factor in categories:
        for item in page.locator(
            f"//*[@id='m_ContentControl_ContentContainer1_ctl00_Content_Section_FINANCIAL_PROFILE_SSCtr']/tbody/tr[{row}]/td[@class='ft WHR WVT']"
        ).all():
            value = item.inner_text()
            if value not in ["n.a.", "n.s.", "n.d.", "nd"]:
                value = into_num(value) * factor
            company.setdefault(key, []).append(value)

    # Tangible assets
    for item in page.locator(
        "//*[@id='m_ContentControl_ContentContainer1_ctl00_Content_Section_BALANCESHEET_DET_SSCtr']/tbody/tr[19]/td[@class='ft WHR WVT']"
    ).all():
        value = item.inner_text()
        if value not in ["n.a.", "n.s.", "n.d"]:
            value = into_num(value) * multiplier
        company.setdefault("tang_assets", []).append(value)


# Main function
def main():
    prev_data = load_previous_data()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=CONFIG["HEADLESS"])
        context = browser.new_context(
            viewport={"width": 1440, "height": 1000}, locale="en-US"
        )
        page = context.new_page()

        login(page)  # Log in to the website

        with jsonlines.open("test_final_f.jsonl", "a") as writer:
            with jsonlines.open("data_ms_bs.jsonl") as reader:
                for company in reader:
                    # Check if already processed
                    if any(prev["isin"] == company["isin"] for prev in prev_data):
                        continue

                    process_company(company, page, writer)

        browser.close()
        logging.info("Finished processing all companies.")


if __name__ == "__main__":
    main()
