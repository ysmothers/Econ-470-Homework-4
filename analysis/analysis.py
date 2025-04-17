# Clean data


final_clean_data = (
    final_data[
        (final_data['year'].between(2010, 2015)) & 
        (final_data['partc_score'].notna())
    ]
    .drop_duplicates(subset=['contractid', 'planid', 'county'])
)
