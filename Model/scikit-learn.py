import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error
from sklearn.ensemble import RandomForestRegressor

# Load your data
df = pd.read_csv("../Data/Merged/merged_players_clean0.csv")


# Target
y = df["market_value_eur"]

# Features: drop the target and unusable fields
X = df.drop(columns=["market_value_eur", "market_value", "dob"])

# Identify categorical columns
categorical_cols = X.select_dtypes(include=["object"]).columns
numeric_cols = X.select_dtypes(exclude=["object"]).columns

# Preprocessing
preprocess = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_cols),
        ("num", "passthrough", numeric_cols)
    ]
)

# Model
model = RandomForestRegressor(n_estimators=500, random_state=42)

# Combine preprocess + model
pipeline = Pipeline(steps=[
    ("preprocess", preprocess),
    ("model", model)
])

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Fit
pipeline.fit(X_train, y_train)

# Predict
preds = pipeline.predict(X_test)

# Evaluate
mae = mean_absolute_error(y_test, preds)
print("MAE:", mae)
