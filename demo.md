
### 1. **Customer and Account Identification**
```markdown
| Feature Name      | Description                                             |
|-------------------|---------------------------------------------------------|
| ip_id             | Unique identifier for a customer or account holder.     |
| hh_id_in_wh       | Identifier for the household to which a customer belongs.|
```

### 2. **Product Details**
```markdown
| Feature Name      | Description                                 |
|-------------------|---------------------------------------------|
| segmt_prod_type   | Category or type of the banking product.    |
```

### 3. **Temporal Information**
```markdown
| Feature Name                  | Description                                                 |
|-------------------------------|-------------------------------------------------------------|
| business_date                 | The reference date for the business transaction data.       |
| open_date                     | Date when the account or product was opened.                |
| one_year_before_open_date     | Date exactly one year prior to the account or product opening.|
```

### 4. **Binary Indicators**
```markdown
| Feature Name                              | Description                                                           |
|-------------------------------------------|-----------------------------------------------------------------------|
| flag                                      | A binary indicator, possibly denoting a specific state of an account.|
| nonrealestatedeveloper                    | Indicates if a customer is not a real estate developer (binary).      |
| incomelevelsge500k                        | Indicates if a customer's income is above $500,000 (binary).          |
| indtrustinvestableassetclient             | Indicates if a client has investable assets in a trust (binary).      |
| indtrustnaturalresourceclient             | Indicates if a client is involved in natural resources (binary).      |
| indtrustrealestateclient                  | Indicates if a client has real estate holdings in a trust (binary).   |
| indcreditclient                           | Indicates if the client has any credit products with the bank (binary).|
| indtrustclient                            | Indicates if the client is a trust account holder (binary).           |
| ind_date_of_business_financials_within_2yr| Indicates if business financials are within two years (binary).       |
| ind_date_of_bureau_pull_within_2yr        | Indicates if the credit bureau pull is within two years (binary).     |
```

### 5. **Customer Characteristics**
```markdown
| Feature Name          | Description                                        |
|-----------------------|----------------------------------------------------|
| industrypreference    | Industry with which the customer is associated.    |
| professionpractice    | Profession or practice area of the customer.       |
```

### 6. **Account Balance Features**
```markdown
| Feature Name                     | Description                                                         |
|----------------------------------|---------------------------------------------------------------------|
| prod_avg_bal_1m                  | Average balance of the product in the past month.                   |
| prod_balance_std_1m              | Standard deviation of the product balance in the past month.        |
| prod_avg_bal_3m                  | Average balance over the past 3 months.                             |
| prod_balance_std_3m              | Standard deviation of the balance over the past 3 months.           |
| prod_avg_bal_6m                  | Average balance over the past 6 months.                             |
| prod_balance_std_6m              | Standard deviation of the balance over the past 6 months.           |
| prod_avg_bal_3_6m                | Average balance from 3 to 6 months ago.                             |
| prod_balance_std_3_6m            | Standard deviation of the balance from 3 to 6 months ago.           |
| prod_avg_bal_3m_minus_3_6m       | Difference in the average balance between the last 3 and 3-6 months.|
| prod_avg_bal_3m_minus_3_6m_pct   | Percentage change in the average balance between the last 3 and 3-6 months.|
```

### 7. **Revenue Features**
```markdown
| Feature Name                     | Description                                                                   |
|----------------------------------|-------------------------------------------------------------------------------|
| revenue_amt_1m                   | Revenue amount in the last month.                                             |
| revenue_amt_std_1m               | Standard deviation of the revenue amount in the last month.                   |
| revenue_amt_3m                   | Revenue amount over the past 3 months.                                        |
| revenue_amt_std_3m               | Standard deviation of the revenue over the past 3 months.                     |
| revenue_amt_6m                   | Revenue amount over the past 6 months.                                        |
| revenue_amt_std_6m               | Standard deviation of the revenue over the past 6 months.                     |
| revenue_amt_12m                  | Revenue amount over the past 12 months.                                       |
| revenue_amt_std_12m              | Standard deviation of the revenue over the past 12 months.                    |
| revenue_amt_3_6m

                 | Revenue amount from 3 to 6 months ago.                                        |
| revenue_amt_std_3_6m             | Standard deviation of the revenue from 3 to 6 months ago.                     |
| revenue_amt_6_12m                | Revenue amount from 6 to 12 months ago.                                       |
| revenue_amt_std_6_12m            | Standard deviation of the revenue from 6 to 12 months ago.                    |
| revenue_amt_3m_minus_3_6m        | Difference in revenue between the last 3 months and the 3 to 6 months prior.   |
| revenue_amt_3m_minus_3_6m_pct    | Percentage change in revenue between the last 3 months and the 3 to 6 months prior.|
| revenue_amt_6m_minus_6_12m       | Difference in revenue between the last 6 months and the 6 to 12 months prior.  |
| revenue_amt_6m_minus_6_12m_pct   | Percentage change in revenue between the last 6 months and the 6 to 12 months prior.|
```

### 8. **Demographic Information**
```markdown
| Feature Name      | Description                             |
|-------------------|-----------------------------------------|
| st_code           | State code, indicating customer location.|
| city_name         | Name of the city where the customer resides.|
```

### 9. **Transaction Timeliness**
```markdown
| Feature Name                      | Description                                      |
|-----------------------------------|--------------------------------------------------|
| dayssincebusinessfinancials       | Days since the last submission of business financials.|
| dayssincebureaupull               | Days since the last credit bureau report was pulled.|
```

### 10. **Debt and Credit Features**
```markdown
| Feature Name                          | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| app_total_debt_serv_ratio_credit_bureau | Applicant's total debt service ratio from credit bureau.|
```

Each table is formatted for clarity and focuses on a specific group of features, making your presentation organized and informative.
