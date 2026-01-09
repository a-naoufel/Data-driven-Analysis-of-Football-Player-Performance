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
# results = []

# for name, model in models.items():
#     print(f"\nTraining {name}...")
#     model.fit(X_train, y_train)
#     y_pred = model.predict(X_test)

#     mae = mean_absolute_error(y_test, y_pred)
#     r2 = r2_score(y_test, y_pred)
#     results.append((name, mae, r2))

#     print(f"{name} -> MAE: {mae:.2f} | R²: {r2:.4f}")

# # 6. Sort and print summary
# print("\n=== Model Comparison (sorted by MAE) ===")
# for name, mae, r2 in sorted(results, key=lambda x: x[1]):
#     print(f"{name:20s}  MAE: {mae:10.2f} | R²: {r2:8.4f}")
import numpy as np
import matplotlib.pyplot as plt

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.base import clone
from sklearn.metrics import r2_score, mean_absolute_error

# -------------------------------------------------------------------
# 0) (Optionnel mais recommandé) Pipelines pour modèles sensibles à l’échelle
#    (SVR / régressions linéaires). Pour RF/GB/XGB pas nécessaire.
# -------------------------------------------------------------------
models = {
    "XGBRegressor": models["XGBRegressor"],
    "RandomForest": models["RandomForest"],
    "GradientBoosting": models["GradientBoosting"],
    "HistGradientBoosting": models["HistGradientBoosting"],
    "LinearRegression": make_pipeline(StandardScaler(), LinearRegression()),
    "Ridge": make_pipeline(StandardScaler(), Ridge(alpha=10.0)),
    "Lasso": make_pipeline(StandardScaler(), Lasso(alpha=1e5, max_iter=10000)),
    "SVR_RBF": make_pipeline(StandardScaler(), SVR(kernel="rbf", C=10.0, epsilon=0.1)),
}

# Si X_train/X_test sont des DataFrames pandas
X_train_np = X_train.values
X_test_np  = X_test.values
y_train_np = y_train.values
y_test_np  = y_test.values

# -------------------------------------------------------------------
# 1) Définir les tailles d’entraînement (en nombre de lignes)
# -------------------------------------------------------------------
n_train = X_train_np.shape[0]
train_sizes = np.unique(np.linspace(200, n_train, 12, dtype=int))  # ajuste 200 et 12 si besoin
train_sizes = train_sizes[train_sizes <= n_train]

# Pour reproductibilité : on mélange une fois puis on prend les k premières lignes
rng = np.random.RandomState(42)
perm = rng.permutation(n_train)

# -------------------------------------------------------------------
# 2) Calcul learning curves (R² + MAE) pour chaque modèle
# -------------------------------------------------------------------
curves = {}  # name -> dict(train_sizes, r2_list, mae_list)

for name, model in models.items():
    r2_list, mae_list = [], []
    print(f"\nLearning curve: {name}")

    for k in train_sizes:
        idx = perm[:k]
        Xk, yk = X_train_np[idx], y_train_np[idx]

        m = clone(model)
        m.fit(Xk, yk)
        pred = m.predict(X_test_np)

        r2_list.append(r2_score(y_test_np, pred))
        mae_list.append(mean_absolute_error(y_test_np, pred))

        print(f"  k={k:6d} | R²={r2_list[-1]:.4f} | MAE={mae_list[-1]:.2f}")

    curves[name] = {"train_sizes": train_sizes, "r2": r2_list, "mae": mae_list}

# -------------------------------------------------------------------
# 3) Plots
# -------------------------------------------------------------------
plt.figure(figsize=(10, 6))
for name, d in curves.items():
    plt.plot(d["train_sizes"], d["r2"], marker="o", linewidth=2, label=name)
plt.xlabel("Nombre de lignes d'entraînement")
plt.ylabel("R² sur le test")
plt.title("Convergence (Learning Curve) - R² vs taille d'entraînement")
plt.grid(True, alpha=0.3)
plt.legend()
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
for name, d in curves.items():
    plt.plot(d["train_sizes"], d["mae"], marker="o", linewidth=2, label=name)
plt.xlabel("Nombre de lignes d'entraînement")
plt.ylabel("MAE sur le test (plus bas = mieux)")
plt.title("Convergence (Learning Curve) - MAE vs taille d'entraînement")
plt.grid(True, alpha=0.3)
plt.legend()
plt.tight_layout()
plt.show()
