"Good [morning/afternoon], everyone. Thank you for joining me today. I’m excited to share our new product recommender model with you. Let’s start by discussing the core challenge we faced: developing a generalized recommendation system that can learn complex, many-to-many relationships between users and items. This task is particularly challenging because it involves understanding and predicting user preferences across a wide range of products.

To tackle this, we chose the DeepFM algorithm, which stands for Deep Factorization Machine. Now, you might be wondering, why did we choose DeepFM?

First, let’s talk about feature interaction. The DeepFM algorithm combines two powerful components:

The Factorization Machine (FM) component handles linear and pairwise interactions efficiently. For example, it can identify simple relationships between features like a product being frequently opened by a certain user segment.
The Deep Neural Network (DNN) component captures more complex, non-linear interactions. This means it can understand deeper relationships in the data that might not be immediately obvious. DNN is a part of deep learning, which is a very powerful technique. Most modern AI applications, from image recognition to natural language processing, use deep learning because of its ability to handle complex patterns and large amounts of data.
Second, combining FM and DNN components results in improved accuracy. DeepFM leverages the strengths of both methods, leading to more accurate and robust predictions. This combination is key to enhancing the model's performance in identifying the best product recommendations for our clients.

Business Impact:

Better Product Prediction: Our model is now more adept at identifying potential products for each client with improved accuracy, which directly translates to better service and satisfaction for our clients.
Scalable and Efficient: The model is capable of handling large datasets with many features. This scalability ensures that as our data grows, the model remains effective and efficient."
Slide 2: Target Definition
"Our target is to scan the entire product span of our Private Wealth portfolio and group similar products into types that make sense for our business objectives. The product categorization was provided by our business team, and we identified 28 distinct product types.

This table here shows the diversity of products, ranging from various types of checking accounts to different loan products. By categorizing products in this way, we ensure our model can provide precise recommendations tailored to specific client needs.

For instance, our categories include different deposit products like money market accounts and various checking account types. On the loan side, we have products like home equity lines of credit (HELOCs) and mortgages. This categorization helps us better understand the product landscape and predict which products will be most relevant to each client."

Slide 3: Feature Selection and Engineering
"In feature selection and engineering, our goal is to capture the interaction between client features and product features. We use approximately 130 features to learn about client preferences effectively.

Client Features:

Demographic Information: For example, if a customer’s income is above $500,000, it can influence their product preferences.
Bureau Debt: This includes the applicant’s total debt service ratio from the credit bureau, which helps us understand their financial behavior.
Revenue: We look at the variance in revenue for a product over the past three months to gauge its stability and attractiveness.
Balance: We consider the percentage difference between recent three-month average balance and the previous three months’ average balance to understand changes in financial behavior.
Product Distribution: For instance, the number of active ROTH IRA clients gives us insight into the popularity and reach of certain products.
Product Features:

Product Balance: This is the mean balance of the product over the last 12 months, helping us gauge its performance.
Client Distribution: We look at the number of clients who opened a particular product in the last six months to understand its recent demand.
By considering these features, our model can make more informed predictions about which products are likely to interest each client."

Slide 4: Data Sourcing
"Finally, let’s talk about data sourcing. To build our model, we pull data from several comprehensive sources. Each source provides different types of information that are crucial for our model’s accuracy.

For example:

From PW R360, we get data on balance, revenue, product distribution, and product information. This gives us a complete financial picture of each client.
The Credit Risk data source includes information on risk levels, account maturity dates, and financial portfolios, including assets and liabilities. This helps us understand the financial health and risk profile of our clients.
Trust data includes daily transactions and fees, as well as the monthly market value of trust accounts. This is essential for understanding the performance and management of trust accounts.
Loan Origination provides client application history and debt information, giving us insight into clients’ borrowing behaviors and creditworthiness.
By integrating data from these diverse sources, our model has a robust foundation, allowing it to make well-informed recommendations. This comprehensive approach ensures we understand our clients from multiple dimensions, enabling us to serve them better."
