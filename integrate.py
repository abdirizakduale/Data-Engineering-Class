import pandas as pd
from us_state_abbrev import abbrev_to_us_state
import seaborn as sns
import matplotlib.pyplot as plt

"""Step 1: Load and Trim Data"""
# Load CSV files
covid_cases   = pd.read_csv('covid_confirmed_usafacts.csv')
covid_deaths  = pd.read_csv('covid_deaths_usafacts.csv')
census_data   = pd.read_csv('acs2017_county_data.csv')

# Keep only required columns
covid_cases   = covid_cases[['County Name', 'State', '2023-07-23']]
covid_deaths  = covid_deaths[['County Name', 'State', '2023-07-23']]
census_data   = census_data[['County', 'State', 'TotalPop', 'IncomePerCap', 'Poverty', 'Unemployment']]

# Show column names
print('[INFO] Columns in loaded data:')
print('  - Cases:', covid_cases.columns.tolist())
print('  - Deaths:', covid_deaths.columns.tolist())
print('  - Census:', census_data.columns.tolist())

"""Step 2: Clean County Names"""
covid_cases['County Name']  = covid_cases['County Name'].str.rstrip()
covid_deaths['County Name'] = covid_deaths['County Name'].str.rstrip()

# Check how many "Washington County" rows exist
print('\n[CHECK] Washington County entries:')
print('  - Cases:', len(covid_cases[covid_cases['County Name'] == 'Washington County']))
print('  - Deaths:', len(covid_deaths[covid_deaths['County Name'] == 'Washington County']))

initial_case_count  = len(covid_cases)
initial_death_count = len(covid_deaths)

"""Step 3: Filter Out Unallocated Entries"""
covid_cases  = covid_cases[covid_cases['County Name'] != 'Statewide Unallocated']
covid_deaths = covid_deaths[covid_deaths['County Name'] != 'Statewide Unallocated']

print('\n[INFO] Records after removing unallocated rows:')
print(f'  - Cases: {initial_case_count} → {len(covid_cases)}')
print(f'  - Deaths: {initial_death_count} → {len(covid_deaths)}')

"""Step 4: Expand Abbreviated States"""
covid_cases['State']  = covid_cases['State'].map(abbrev_to_us_state)
covid_deaths['State'] = covid_deaths['State'].map(abbrev_to_us_state)

print('\n[PREVIEW] Sample of case data after state mapping:')
print(covid_cases.head())

"""Step 5: Create Join Key"""
covid_cases['Location']  = covid_cases['County Name'] + ', ' + covid_cases['State']
covid_deaths['Location'] = covid_deaths['County Name'] + ', ' + covid_deaths['State']
census_data['Location']  = census_data['County'] + ', ' + census_data['State']

covid_cases.set_index('Location', inplace=True)
covid_deaths.set_index('Location', inplace=True)
census_data.set_index('Location', inplace=True)

print('\n[PREVIEW] Census data with new index:')
print(census_data.head())

"""Step 6: Rename Columns"""
covid_cases.rename(columns={'2023-07-23': 'ConfirmedCases'}, inplace=True)
covid_deaths.rename(columns={'2023-07-23': 'ConfirmedDeaths'}, inplace=True)

print('\n[INFO] Updated column names:')
print('  - Cases:', covid_cases.columns.tolist())
print('  - Deaths:', covid_deaths.columns.tolist())

"""Step 7: Merge Datasets"""
combined_df = (
    covid_cases[['ConfirmedCases']]
    .join(covid_deaths[['ConfirmedDeaths']], how='inner')
    .join(census_data[['TotalPop', 'IncomePerCap', 'Poverty', 'Unemployment']], how='inner')
)

combined_df['CasesPerCap']  = combined_df['ConfirmedCases'] / combined_df['TotalPop']
combined_df['DeathsPerCap'] = combined_df['ConfirmedDeaths'] / combined_df['TotalPop']

print(f'\n[INFO] Final combined dataset contains {len(combined_df)} rows.')

"""Step 8: Correlation Analysis"""
correlations = combined_df.corr()
print('\n[RESULT] Correlation Matrix:\n')
print(correlations.round(2))

"""Optional: Correlation Heatmap Visualization"""
plt.figure(figsize=(10, 8))
sns.heatmap(
    correlations,
    annot=True,
    cmap='coolwarm',
    fmt='.2f',
    linewidths=0.5
)
plt.title('Correlation Matrix Heatmap')
plt.tight_layout()
plt.show()
