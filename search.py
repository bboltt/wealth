import os
import pandas as pd

# Step 1: Load the CSV file
csv_file = "your_file.csv"  # Replace with your file name
df = pd.read_csv(csv_file)

# Ensure the column has no leading/trailing spaces and drop duplicates
df['values'] = df['values'].str.strip()  # Assuming the column name is 'values'
values = df['values'].unique()

# Step 2: Initialize lists for found and not found values
values_in_files = []
values_not_in_files = list(values)

# Step 3: Search for values (as substrings) in all files except the CSV file
for root, dirs, files in os.walk("."):
    for file_name in files:
        # Skip the CSV file itself
        if file_name == os.path.basename(csv_file):
            continue

        file_path = os.path.join(root, file_name)
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                # Check each value as a substring in the file content
                for value in values_not_in_files[:]:  # Copy to allow modification
                    if value in content:
                        values_in_files.append(value)
                        values_not_in_files.remove(value)  # Remove to avoid duplicate checks
        except (IOError, UnicodeDecodeError):
            # Skip files that can't be read as text
            continue

# Step 4: Display the results
print("Values found within files:")
print(values_in_files)

print("\nValues not found within any files:")
print(values_not_in_files)

