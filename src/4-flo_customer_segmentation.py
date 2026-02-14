import datetime as dt
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.float_format', lambda x: "%.4f" %x)

############################## GÖREV 1 ######################################
#Adım 1
df_ = pd.read_csv("datasets/flo_data_20k.csv")
df = df_.copy()

#Adım 2
df.head(10)

df["order_channel"].value_counts()
df["last_order_channel"].value_counts()

df.columns
df.shape
df.describe().T
df.isnull().sum()
df.info()

#Adım 3
df["total_order_num"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]

df["total_customer_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

#Adım 4
date_cols = [col for col in df.columns if "_date" in col]

for col in date_cols:
    df[col] = pd.to_datetime(df[col])

df.info()

#Adım 5
df.groupby("order_channel").agg({"master_id": "count",
                                 "total_order_num": "sum",
                                 "total_customer_value": "sum"})

#Adım 6
df.sort_values(by="total_customer_value", ascending=False).head(10)

#Adım 7
df.sort_values(by="total_order_num", ascending=False).head(10)

#Adım 8
def eda(df):
    df["total_order_num"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]

    df["total_customer_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

    # Adım 4
    date_cols = [col for col in df.columns if "_date" in col]

    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    df.groupby("order_channel").agg({"master_id": "count",
                                     "total_order_num": "sum",
                                     "total_customer_value": "sum"})

    print("Parasal getirisi en yüksek olan 10 müşteri:", df.sort_values(by="total_customer_value", ascending=False).head(10).index)

    print("En fazla alışveriş yapan 10 müşteri: ", df.sort_values(by="total_order_num", ascending=False).head(10).index)

############################## GÖREV 2 ######################################
#Adım 1
#Recency: Müşterinin en son alışveriş yaptığı tarihten bu güne kadar geçen süre
#Frequency: Müşterinin yaptığı alışveriş sayısı
#Monetary: Müşterinin yaptığı alışverişler sonucu yaptığı toplam ödeme

#Adım 2
rfm = pd.DataFrame()
today = dt.datetime.today()
rfm["Recency"] = (today - df["last_order_date"]).dt.days
rfm["Frequency"] = df["total_order_num"].astype(int)
rfm["Monetary"] = df["total_customer_value"].astype(int)

rfm.index = df.master_id
rfm.head()

############################## GÖREV 3 ######################################
#Adım 1
rfm["recency_score"] = pd.qcut(rfm["Recency"], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["Frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm["Monetary"], 5, labels=[1, 2, 3, 4, 5])

#Adım 2
rfm["RF_SCORE"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)

############################## GÖREV 4 ######################################
#Adım 1
seg_map = {
        r'[1-2][1-2]': "hibernating",
        r'[1-2][3-4]': "at_Risk",
        r'[1-2][5]': "cant_loose",
        r'[3][1-2]': "about_to_sleep",
        r'[3][3]': "need_attention",
        r'[3-4][4-5]': "loyal_customers",
        r'[4][1]': "promising",
        r'[5][1]': "new_customers",
        r'[4-5][2-3]': "potential_loyalist",
        r'[5][4-5]': "champions",
    }

#Adım 2
rfm["segment"] = rfm["RF_SCORE"].replace(seg_map, regex=True)

############################## GÖREV 5 ######################################
#Adım 1
rfm[["segment", "Recency", "Frequency", "Monetary"]].groupby("segment").mean()

#Adım 2
rfm.reset_index(inplace=True)
df_final = df.merge(rfm, on="master_id", how="left")

#a)
loyal_woman_customers_df = df_final[(df_final["segment"].isin(["champions", "loyal_customers"])) &
                               (df_final["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]

loyal_woman_customers_df.to_csv("flo_kadin_markası_hedef_müsteriler.csv", index=False)

#b)
discount_customers = df_final[(df_final["segment"].isin(["cant_loose", "atrisk", "hibernating", "new_customers"])) &
                              (df_final["interested_in_categories_12"].str.contains("ERKEK|ÇOCUK", na=False))]["master_id"]

discount_customers.to_csv("hedef_indirim_müsterileri.csv", index=False)

df.head()
df.columns
df_final.head()
rfm.head()
rfm.columns
