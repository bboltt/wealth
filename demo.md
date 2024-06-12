### Slide 5: Introduction to DeepFM Model

**DeepFM Overview:**
- **DeepFM (Deep Factorization Machine):**
  - Combines Factorization Machines (FM) and Deep Neural Networks (DNN).
  - Efficiently handles both sparse (categorical) and dense (numerical) features.
  - Captures both low-order and high-order feature interactions.

**Key Components:**
1. **Factorization Machine (FM):**
   - Captures **low-order feature interactions**.
   - Efficiently handles sparse data by embedding features into dense vectors.
   - Useful for identifying simple relationships between features.

2. **Deep Neural Network (DNN):**
   - Captures **high-order feature interactions**.
   - Multiple layers of neurons allow for complex pattern recognition.
   - Enhances the model's ability to understand intricate data relationships.

**Why DeepFM?**
- **Comprehensive Feature Interaction:**
  - FM component captures linear and pairwise interactions.
  - DNN component captures complex, non-linear interactions.
- **Improved Accuracy:**
  - By combining FM and DNN, DeepFM leverages the strengths of both methods.
  - Results in more accurate and robust predictions.

**Business Impact:**
- **Better Customer Prospecting:**
  - Enhanced ability to identify potential PWM customers through improved prediction accuracy.
- **Scalable and Efficient:**
  - Capable of handling large datasets with high-dimensional features.
- **Versatile Applications:**
  - Can be applied to various recommendation and prediction tasks beyond customer prospecting.

---

This slide introduces the DeepFM model, highlighting its components, advantages, and potential business impacts.





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
