import pandas as pd

# If your data is in a CSV file, load it using:
# final_data = pd.read_csv('path_to_your_file.csv')

# For demonstration, let's assume final_data is already loaded
# Proceed to clean the data
final_clean_data = final_data[
    (final_data['year'].between(2010, 2015)) &
    (~final_data['partc_score'].isna()) &
    (final_data['plan_type'] != 'SNP') &
    (~final_data['planid'].astype(str).str.startswith('800')) &
    (final_data['partc_benefits'] == True)  # Assuming this column indicates Part C benefits
].drop_duplicates(subset=['contractid', 'planid', 'county'])