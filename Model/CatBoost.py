from catboost import CatBoostRegressor, Pool
import pandas as pd

# Categorical columns
df = pd.read_csv("../Data/Merged/merged_players_clean0.csv")
cat_cols = df.select_dtypes(include="object").columns.tolist()

X = df.drop(columns=["market_value_eur", "market_value", "dob"])
y = df["market_value_eur"]

train_pool = Pool(X, y, cat_features=cat_cols)

model = CatBoostRegressor(iterations=3000, learning_rate=0.03, depth=10, loss_function='MAE')
model.fit(train_pool)

preds = model.predict(X)
