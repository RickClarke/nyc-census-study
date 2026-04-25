"""/// script
dependencies = [
    "censusdis",
    "numpy",
    "pandas",
]
///"""

import censusdis.data as ced
import pandas as pd
import numpy as np
from censusdis import states
import os
from pathlib import Path

# Create the output directory next to this script so `uv run census.py` works
# regardless of the caller's current working directory.
DATA_DIR = Path(__file__).resolve().parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY")

# NYC geographic identifiers - let's try multiple approaches
NYC_STATE = "36"  # New York state FIPS
NYC_COUNTY_CODES = ["005", "047", "061", "081", "085"]  # Bronx, Brooklyn, Manhattan, Queens, Staten Island

def test_api_connection():
    """Test API connection and find correct NYC identifiers"""
    print("Testing Census API connection...")

    if not CENSUS_API_KEY:
        print("✗ API connection failed: CENSUS_API_KEY is not set")
        return False
    
    try:
        # Test basic API connection with a simple query
        test_df = ced.download(
            dataset='acs/acs5',
            vintage=2020,
            download_variables=['B01001_001E'],  # Total population
            state="36"  # New York state
        )
        print("✓ API connection successful")
        print(f"Found {len(test_df)} geographic areas in NY state")
        return True
    except Exception as e:
        print(f"✗ API connection failed: {e}")
        return False

def get_nyc_data_by_counties(variables, year=2020, dataset='acs/acs5'):
    """Get NYC data by aggregating all 5 boroughs (counties)"""
    print(f"Fetching data for NYC boroughs (year {year})...")
    
    borough_data = []
    borough_names = ["Bronx", "Kings", "New York", "Queens", "Richmond"]
    
    for i, county_code in enumerate(NYC_COUNTY_CODES):
        try:
            print(f"  Fetching data for {borough_names[i]} County...")
            df = ced.download(
                dataset=dataset,
                vintage=year,
                download_variables=variables,
                state=NYC_STATE,
                county=county_code
            )
            
            if not df.empty:
                # Add borough identifier
                df['Borough'] = borough_names[i]
                df['County_Code'] = county_code
                borough_data.append(df)
                print(f"    ✓ Successfully fetched {len(df)} records")
            else:
                print(f"    ✗ No data returned for {borough_names[i]}")
                
        except Exception as e:
            print(f"    ✗ Error fetching {borough_names[i]} data: {e}")
            continue
    
    if borough_data:
        combined_df = pd.concat(borough_data, ignore_index=True)
        print(f"✓ Combined data from {len(borough_data)} boroughs")
        return combined_df
    else:
        print("✗ No borough data successfully retrieved")
        return pd.DataFrame()

def aggregate_nyc_totals(borough_df, numeric_columns):
    """Aggregate borough data to get NYC totals"""
    if borough_df.empty:
        return {}
    
    nyc_totals = {}
    for col in numeric_columns:
        if col in borough_df.columns:
            # Sum across all boroughs
            total = borough_df[col].sum()
            nyc_totals[col] = total if pd.notna(total) else 0
    
    return nyc_totals

def get_population_pyramid_data(year):
    """Get population by age and sex for population pyramid"""
    print(f"Fetching population pyramid data for {year}...")
    
    # Age and sex variables
    age_sex_vars = [
        'B01001_002E',  # Male total
        'B01001_003E', 'B01001_004E', 'B01001_005E', 'B01001_006E', 'B01001_007E',
        'B01001_008E', 'B01001_009E', 'B01001_010E', 'B01001_011E', 'B01001_012E',
        'B01001_013E', 'B01001_014E', 'B01001_015E', 'B01001_016E', 'B01001_017E',
        'B01001_018E', 'B01001_019E', 'B01001_020E', 'B01001_021E', 'B01001_022E',
        'B01001_023E', 'B01001_024E', 'B01001_025E',  # Male age groups
        'B01001_026E',  # Female total
        'B01001_027E', 'B01001_028E', 'B01001_029E', 'B01001_030E', 'B01001_031E',
        'B01001_032E', 'B01001_033E', 'B01001_034E', 'B01001_035E', 'B01001_036E',
        'B01001_037E', 'B01001_038E', 'B01001_039E', 'B01001_040E', 'B01001_041E',
        'B01001_042E', 'B01001_043E', 'B01001_044E', 'B01001_045E', 'B01001_046E',
        'B01001_047E', 'B01001_048E', 'B01001_049E'   # Female age groups
    ]
    
    try:
        borough_df = get_nyc_data_by_counties(age_sex_vars, year)
        
        if borough_df.empty:
            return pd.DataFrame()
        
        # Aggregate boroughs
        nyc_totals = aggregate_nyc_totals(borough_df, age_sex_vars)
        
        # Create age groups buckets
        age_groups = [
            'Under 5', '5-9', '10-14', '15-17', '18-19', '20', '21', '22-24',
            '25-29', '30-34', '35-39', '40-44', '45-49', '50-54', '55-59',
            '60-61', '62-64', '65-66', '67-69', '70-74', '75-79', '80-84', '85+'
        ]
        
        pyramid_data = []
        for i, age_group in enumerate(age_groups):
            male_col = f'B01001_{str(i+3).zfill(3)}E'
            female_col = f'B01001_{str(i+27).zfill(3)}E'
            
            male_pop = nyc_totals.get(male_col, 0)
            female_pop = nyc_totals.get(female_col, 0)
            
            pyramid_data.append({
                'Year': year,
                'Age_Group': age_group,
                'Male_Population': male_pop,
                'Female_Population': female_pop,
                'Total_Population_Age_Group': male_pop + female_pop
            })
        
        return pd.DataFrame(pyramid_data)
    
    except Exception as e:
        print(f"Error fetching population pyramid data: {e}")
        return pd.DataFrame()

def get_demographic_data(years):
    """Get demographic data for multiple years"""
    print("Fetching demographic data...")
    
    all_data = []
    
    for year in years:
        print(f"Processing year {year}...")
        try:
            # Basic demographics
            basic_vars = [
                'B01001_001E',  # Total population
                'B01001_002E',  # Male population
                'B01001_026E',  # Female population
                'B01002_001E',  # Median age total
                'B01002_002E',  # Median age male
                'B01002_003E',  # Median age female
            ]
            
            # Race/ethnicity
            race_vars = [
                'B02001_002E',  # White alone
                'B02001_003E',  # Black or African American alone
                'B02001_004E',  # American Indian and Alaska Native alone
                'B02001_005E',  # Asian alone
                'B02001_006E',  # Native Hawaiian and Other Pacific Islander alone
                'B02001_007E',  # Some other race alone
                'B02001_008E',  # Two or more races
                'B03003_003E',  # Hispanic or Latino
            ]
            
            # Income
            income_vars = [
                'B19013_001E',  # Median household income
                'B19301_001E',  # Per capita income
            ]
            
            # Housing
            housing_vars = [
                'B25001_001E',  # Total housing units
                'B25002_002E',  # Occupied housing units
                'B25002_003E',  # Vacant housing units
            ]
            
            all_vars = basic_vars + race_vars + income_vars + housing_vars
            
            borough_df = get_nyc_data_by_counties(all_vars, year)
            
            if borough_df.empty:
                print(f"  ✗ No data for year {year}")
                continue
            
            # For demographic data, we'll use population weighted averages to handle median values.
            
            # Sum totals across boroughs
            numeric_vars = [v for v in all_vars if v not in ['B01002_001E', 'B01002_002E', 'B01002_003E', 'B19013_001E', 'B19301_001E']]
            nyc_totals = aggregate_nyc_totals(borough_df, numeric_vars)
            
            # Calculate population weighted medians for age and income
            total_pop = nyc_totals.get('B01001_001E', 0)
            if total_pop > 0:
                # For median age and income, we'll use a simple average of borough values (This is an approximation)
                median_age_total = borough_df['B01002_001E'].mean() if 'B01002_001E' in borough_df.columns else 0
                median_age_male = borough_df['B01002_002E'].mean() if 'B01002_002E' in borough_df.columns else 0
                median_age_female = borough_df['B01002_003E'].mean() if 'B01002_003E' in borough_df.columns else 0
                median_household_income = borough_df['B19013_001E'].mean() if 'B19013_001E' in borough_df.columns else 0
                per_capita_income = borough_df['B19301_001E'].mean() if 'B19301_001E' in borough_df.columns else 0
            else:
                median_age_total = median_age_male = median_age_female = 0
                median_household_income = per_capita_income = 0
            
            # Extract data
            row_data = {
                'Year': year,
                'Total_Population': nyc_totals.get('B01001_001E', 0),
                'Male_Population': nyc_totals.get('B01001_002E', 0),
                'Female_Population': nyc_totals.get('B01001_026E', 0),
                'Median_Age_Total': median_age_total,
                'Median_Age_Male': median_age_male,
                'Median_Age_Female': median_age_female,
                'White_Alone': nyc_totals.get('B02001_002E', 0),
                'Black_Alone': nyc_totals.get('B02001_003E', 0),
                'American_Indian_Alaska_Native': nyc_totals.get('B02001_004E', 0),
                'Asian_Alone': nyc_totals.get('B02001_005E', 0),
                'Native_Hawaiian_Pacific_Islander': nyc_totals.get('B02001_006E', 0),
                'Other_Race_Alone': nyc_totals.get('B02001_007E', 0),
                'Two_Or_More_Races': nyc_totals.get('B02001_008E', 0),
                'Hispanic_Latino': nyc_totals.get('B03003_003E', 0),
                'Median_Household_Income': median_household_income,
                'Per_Capita_Income': per_capita_income,
                'Total_Housing_Units': nyc_totals.get('B25001_001E', 0),
                'Occupied_Housing_Units': nyc_totals.get('B25002_002E', 0),
                'Vacant_Housing_Units': nyc_totals.get('B25002_003E', 0),
            }
            
            # Calculate percentages
            total_pop = row_data['Total_Population']
            if total_pop > 0:
                row_data['Male_Percentage'] = (row_data['Male_Population'] / total_pop) * 100
                row_data['Female_Percentage'] = (row_data['Female_Population'] / total_pop) * 100
                row_data['White_Percentage'] = (row_data['White_Alone'] / total_pop) * 100
                row_data['Black_Percentage'] = (row_data['Black_Alone'] / total_pop) * 100
                row_data['Asian_Percentage'] = (row_data['Asian_Alone'] / total_pop) * 100
                row_data['Hispanic_Latino_Percentage'] = (row_data['Hispanic_Latino'] / total_pop) * 100
            
            # Calculate housing occupancy rate
            total_housing = row_data['Total_Housing_Units']
            if total_housing > 0:
                row_data['Housing_Occupancy_Rate'] = (row_data['Occupied_Housing_Units'] / total_housing) * 100
                row_data['Housing_Vacancy_Rate'] = (row_data['Vacant_Housing_Units'] / total_housing) * 100
            
            # NYC area is approximately 778.2 km²
            nyc_area_km2 = 778.2
            row_data['Population_Density_Per_Km2'] = total_pop / nyc_area_km2
            
            all_data.append(row_data)
            print(f"  ✓ Successfully processed year {year}")
            
        except Exception as e:
            print(f"  ✗ Error processing year {year}: {e}")
            continue
    
    return pd.DataFrame(all_data)

def get_additional_demographics(year):
    """Get additional demographic details"""
    print(f"Fetching additional demographics for {year}...")
    
    try:
        # Educational attainment
        education_vars = [
            'B15003_001E',  # Total population 25 years and over
            'B15003_017E',  # Regular high school diploma
            'B15003_021E',  # Bachelor's degree
            'B15003_022E',  # Master's degree
            'B15003_023E',  # Professional degree
            'B15003_024E',  # Doctorate degree
        ]
        
        # Employment status
        employment_vars = [
            'B23025_001E',  # Total civilian labor force
            'B23025_002E',  # In labor force
            'B23025_005E',  # Unemployed
        ]
        
        # Poverty status
        poverty_vars = [
            'B17001_001E',  # Total population for poverty determination
            'B17001_002E',  # Below poverty level
        ]
        
        all_vars = education_vars + employment_vars + poverty_vars
        
        borough_df = get_nyc_data_by_counties(all_vars, year)
        
        if borough_df.empty:
            return pd.DataFrame()
        
        # Aggregate across boroughs
        nyc_totals = aggregate_nyc_totals(borough_df, all_vars)
        
        additional_data = {
            'Year': year,
            'Total_25_Plus': nyc_totals.get('B15003_001E', 0),
            'High_School_Graduates': nyc_totals.get('B15003_017E', 0),
            'Bachelors_Degree': nyc_totals.get('B15003_021E', 0),
            'Masters_Degree': nyc_totals.get('B15003_022E', 0),
            'Professional_Degree': nyc_totals.get('B15003_023E', 0),
            'Doctorate_Degree': nyc_totals.get('B15003_024E', 0),
            'Civilian_Labor_Force': nyc_totals.get('B23025_001E', 0),
            'In_Labor_Force': nyc_totals.get('B23025_002E', 0),
            'Unemployed': nyc_totals.get('B23025_005E', 0),
            'Total_Poverty_Universe': nyc_totals.get('B17001_001E', 0),
            'Below_Poverty_Level': nyc_totals.get('B17001_002E', 0),
        }
        
        # Calculate rates
        if additional_data['Civilian_Labor_Force'] > 0:
            additional_data['Unemployment_Rate'] = (additional_data['Unemployed'] / additional_data['Civilian_Labor_Force']) * 100
        
        if additional_data['Total_Poverty_Universe'] > 0:
            additional_data['Poverty_Rate'] = (additional_data['Below_Poverty_Level'] / additional_data['Total_Poverty_Universe']) * 100
        
        return pd.DataFrame([additional_data])
    
    except Exception as e:
        print(f"Error fetching additional demographics: {e}")
        return pd.DataFrame()

def main():
    """Main function to extract all NYC census data"""
    print("Starting NYC Census Data Extraction...")
    print("=" * 50)
    
    # Test API connection first
    if not test_api_connection():
        print("Please check your Census API key and try again.")
        return
    
    # Set years parameters
    pyramid_year = 2020
    demographic_years = range(2015, 2021)
    additional_year = 2020
    
    # Get population pyramid data (most recent year)
    pyramid_df = get_population_pyramid_data(pyramid_year)
    
    # Get demographic data for last 5 years
    demographic_df = get_demographic_data(demographic_years)
    
    # Get additional demographics
    additional_df = get_additional_demographics(additional_year)
    
    # Export to CSV files in data folder
    print("\nExporting data to CSV files in 'data' folder...")
    
    if not pyramid_df.empty:
        pyramid_filename = DATA_DIR / f'nyc_population_pyramid_{pyramid_year}.csv'
        pyramid_df.to_csv(pyramid_filename, index=False)
        print(f"✓ Population pyramid data exported to '{pyramid_filename}'")
    else:
        print("✗ No population pyramid data to export")
    
    if not demographic_df.empty:
        demo_start_year = min(demographic_years)
        demo_end_year = max(demographic_years)
        demographic_filename = DATA_DIR / f'nyc_demographics_{demo_start_year}_{demo_end_year}.csv'
        demographic_df.to_csv(demographic_filename, index=False)
        print(f"✓ Demographics data exported to '{demographic_filename}'")
    else:
        print("✗ No demographics data to export")
    
    if not additional_df.empty:
        additional_filename = DATA_DIR / f'nyc_additional_demographics_{additional_year}.csv'
        additional_df.to_csv(additional_filename, index=False)
        print(f"✓ Additional demographics exported to '{additional_filename}'")
    else:
        print("✗ No additional demographics data to export")
    
    # Create a summary report
    if not demographic_df.empty:
        print("\n" + "=" * 50)
        print("NYC DEMOGRAPHICS SUMMARY (Most Recent Year)")
        print("=" * 50)
        
        latest_data = demographic_df[demographic_df['Year'] == demographic_df['Year'].max()].iloc[0]
        
        print(f"Total Population: {latest_data['Total_Population']:,}")
        print(f"Population Density: {latest_data['Population_Density_Per_Km2']:,.1f} people per km²")
        print(f"Male: {latest_data['Male_Percentage']:.1f}% | Female: {latest_data['Female_Percentage']:.1f}%")
        print(f"Median Age: {latest_data['Median_Age_Total']:.1f} years")
        print(f"Median Household Income: ${latest_data['Median_Household_Income']:,.0f}")
        print(f"Housing Occupancy Rate: {latest_data['Housing_Occupancy_Rate']:.1f}%")
        
        print(f"\nRacial/Ethnic Composition:")
        print(f"  White: {latest_data['White_Percentage']:.1f}%")
        print(f"  Black: {latest_data['Black_Percentage']:.1f}%")
        print(f"  Asian: {latest_data['Asian_Percentage']:.1f}%")
        print(f"  Hispanic/Latino: {latest_data['Hispanic_Latino_Percentage']:.1f}%")
    
    print(f"\n{'=' * 50}")
    print("Data extraction completed!")
    print("Check the 'data' folder for your CSV files.")
    print(f"{'=' * 50}")

if __name__ == "__main__":
    main()