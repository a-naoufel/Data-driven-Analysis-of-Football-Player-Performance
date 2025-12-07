import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, HistGradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.svm import SVR
from xgboost import XGBRegressor

# 1. Load data
df = pd.read_csv("../Data/Merged/merged_players_clean0.csv")

# 2. Target and features
y = df["market_value_eur"]
X = df.drop(columns=["market_value_eur", "market_value", "club"])

# One-hot encode categoricals
X = pd.get_dummies(X, drop_first=True)

# 3. Train / test split (same for all models)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# 4. Define models to compare
models = {
    "XGBRegressor": XGBRegressor(
        n_estimators=600,
        learning_rate=0.03,
        max_depth=7,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2,
        random_state=42
    ),
    "RandomForest": RandomForestRegressor(
        n_estimators=400,
        max_depth=None,
        min_samples_split=2,
        min_samples_leaf=1,
        n_jobs=-1,
        random_state=42
    ),
    "GradientBoosting": GradientBoostingRegressor(
        n_estimators=600,
        learning_rate=0.03,
        max_depth=3,
        random_state=42
    ),
    "HistGradientBoosting": HistGradientBoostingRegressor(
        max_depth=7,
        learning_rate=0.05,
        max_iter=400,
        random_state=42
    ),
    "LinearRegression": LinearRegression(),
    "Ridge": Ridge(alpha=10.0),
    "Lasso": Lasso(alpha=1e5, max_iter=10000),
    # SVR can be slow on large data; comment out if too heavy
    "SVR_RBF": SVR(kernel="rbf", C=10.0, epsilon=0.1)
}

# 5. Fit, predict, evaluate
results = []

for name, model in models.items():
    print(f"\nTraining {name}...")
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    results.append((name, mae, r2))

    print(f"{name} -> MAE: {mae:.2f} | R²: {r2:.4f}")

# 6. Sort and print summary
print("\n=== Model Comparison (sorted by MAE) ===")
for name, mae, r2 in sorted(results, key=lambda x: x[1]):
    print(f"{name:20s}  MAE: {mae:10.2f} | R²: {r2:8.4f}")
