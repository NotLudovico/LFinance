import json
import pandas as pd

# Input and output file names
input_file = "companies.jsonl"
output_file = "data.xlsx"

records = []

# Read and process each JSON record
with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)
        processed_record = {}
        # For each key, if the value is a list, take the first element; otherwise, keep it as is.
        for key, value in record.items():
            if isinstance(value, list):
                processed_record[key] = value[0] if value else None
            else:
                processed_record[key] = value
        records.append(processed_record)

# Create a DataFrame from the processed records
df = pd.DataFrame(records)

# Write DataFrame to Excel using XlsxWriter with nan_inf_to_errors enabled.
# Note: The 'engine_kwargs' parameter passes options to the underlying Workbook.
writer = pd.ExcelWriter(
    output_file,
    engine="xlsxwriter",
    engine_kwargs={"options": {"nan_inf_to_errors": True}},
)
df.to_excel(writer, index=False, sheet_name="Sheet1")

# Access the workbook and worksheet objects
workbook = writer.book
worksheet = writer.sheets["Sheet1"]

# Define formats

# Header format: bold, centered, with a light background and thicker borders.
header_format = workbook.add_format(
    {
        "bold": True,
        "text_wrap": True,
        "align": "center",
        "valign": "vcenter",
        "border": 2,
        "bg_color": "#D7E4BC",  # light green
        "font_size": 12,
    }
)

# Format for odd data rows: white background with thick borders.
format_odd = workbook.add_format({"border": 2, "bg_color": "#FFFFFF"})

# Format for even data rows: light gray background with thick borders.
format_even = workbook.add_format({"border": 2, "bg_color": "#F0F0F0"})

# --- Apply the header format ---
for col_num, col_name in enumerate(df.columns.values):
    worksheet.write(0, col_num, col_name, header_format)

# --- Apply alternating row formats to the data rows ---
# Data rows start at Excel row 1 (since row 0 is the header).
for row_num in range(1, len(df) + 1):
    # Alternate formats: first data row white, second gray, etc.
    row_format = format_odd if (row_num % 2 == 1) else format_even
    for col_num in range(len(df.columns)):
        cell_value = df.iloc[row_num - 1, col_num]
        worksheet.write(row_num, col_num, cell_value, row_format)

# Optionally, adjust column widths based on the maximum length of data in each column.
for i, col in enumerate(df.columns):
    # Find the maximum length among all values in the column and the header.
    max_length = max(df[col].astype(str).map(len).max(), len(str(col)))
    worksheet.set_column(i, i, max_length + 2)

# Save and close the Excel file.
writer.close()

print(f"Data successfully exported to {output_file}")
