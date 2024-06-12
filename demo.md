### Slide 6: Model Training and Evaluation Workflow

**Workflow Overview:**

1. **Data Labeling:**
   - **Positive Labels (1):** Records where a client opened a product.
   - **Negative Labels (0):** Client-product pairs that were never opened in the target period.

2. **Handling Data Imbalance:**
   - **Stratified Subsampling:**
     - Balance the data by subsampling the negative labels (0) to match the distribution of positive labels (1).

3. **Data Preprocessing:**
   - **Feature Handling:**
     - Drop unnecessary columns: `business_date`, `open_date`, `one_year_before_open_date`.
     - Identify and process:
       - **Sparse Features:** Categorical features (e.g., `ip_id`, `Segmt_Prod_Type`).
       - **Dense Features:** Numerical features.
   - **Encoding and Scaling:**
     - Encode sparse features using `OrdinalEncoder`.
     - Scale dense features using `MinMaxScaler`.
     - Generate additional features: `duplicate_ip_id`, `duplicate_Segmt_Prod_Type`.

4. **Model Training:**
   - Split the preprocessed data into training and validation sets.
   - Define feature columns:
     - **Sparse Features:** Use `SparseFeat` with embedding dimensions to capture feature interactions.
     - **Dense Features:** Use `DenseFeat`.
   - Build and compile the DeepFM model using defined feature columns.
     - **Embedding Layers:** Transform sparse features into dense vectors to capture interactions.
   - Train the model with the training data.

5. **Model Evaluation:**
   - Evaluate model performance on both training and validation sets using out-of-sample data.
   - **Metrics Used:**
     - **AUC (Area Under the Curve):** Measures the ability of the model to distinguish between classes.
     - **Lift Score:** Measures the effectiveness of the model in ranking positive instances higher.

**Visualization:**

![Model Pipeline Flow](https://yourimageurl.com/pipeline_flow.png) *(Replace with actual pipeline diagram image)*

**Key Benefits:**
- **Embeddings:** Efficiently capture feature interactions, enhancing model performance.
- **Stratified Subsampling:** Ensure balanced data for training, improving model robustness.
- **Accurate Predictions:** Enhanced product recommendations with precise targeting.

---

This slide provides a clear and detailed overview of your specific model training and evaluation workflow, including how you handle data imbalance and the metrics used for evaluation.




### Slide 6: Model Pipeline Overview

**Pipeline Overview:**

1. **Data Collection:**
   - Extract data from:
     - Training table: `sb_dsp.recommendation_train_data_v2`
     - Validation table: `sb_dsp.recommendation_validation_data_v1`

2. **Data Preprocessing:**
   - **Subsampling:**
     - Use stratified subsampling to balance the data with specified zero ratios and seeds.
   - **Feature Handling:**
     - Drop unnecessary columns: `business_date`, `open_date`, `one_year_before_open_date`.
     - Identify and process:
       - **Sparse Features:** Categorical features (including `ip_id` and other string columns).
       - **Dense Features:** Numerical features.

3. **Feature Engineering:**
   - Encode sparse features using `OrdinalEncoder`.
   - Scale dense features using `MinMaxScaler`.
   - Generate additional features: `duplicate_ip_id`, `duplicate_Segmt_Prod_Type`.

4. **Model Training:**
   - Split the preprocessed data into training and validation sets.
   - Define feature columns:
     - **Sparse Features:** Use `SparseFeat` with embedding dimensions to capture feature interactions.
     - **Dense Features:** Use `DenseFeat`.
   - Build and compile the DeepFM model using defined feature columns.
     - **Embedding Layers:** Transform sparse features into dense vectors to capture interactions.
   - Train the model with the training data.

5. **Model Evaluation:**
   - Evaluate model performance on both training and validation sets.
   - Calculate evaluation metrics: Log Loss, ROC-AUC Score.
   - Generate product-level performance summaries.

6. **Testing on New Data:**
   - Apply the trained model to new validation data (`sb_dsp.recommendation_validation_data_v1`).
   - Process new data using the same encoding and scaling techniques.
   - Evaluate and summarize model performance on new data.

**Visualization:**

![Model Pipeline Flow](https://yourimageurl.com/pipeline_flow.png) *(Replace with actual pipeline diagram image)*

**Key Benefits:**
- **Embeddings:** Efficiently capture feature interactions, enhancing model performance.
- **Sparse and Dense Feature Handling:** Robustly manage different data types.
- **Stratified Subsampling:** Ensure balanced data for training, improving model robustness.
- **Accurate Predictions:** Enhance customer prospecting with precise targeting.

---

This slide provides a clear and detailed overview of your specific model pipeline based on the code you provided, including the role of embeddings in the DeepFM model.
