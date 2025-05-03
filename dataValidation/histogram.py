import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('employees.csv')
plt.hist(df['salary'].dropna(), bins=30)
plt.title('Salary Distribution')
plt.xlabel('Salary')
plt.ylabel('Frequency')
plt.show()
