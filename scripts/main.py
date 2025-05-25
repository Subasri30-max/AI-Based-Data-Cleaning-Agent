from data_ingestions import DataIngestion
from data_cleaning import DataCleaning
from ai_agent import AIAgent

DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="demodb"
DB_USER="postgres"
DB_PASSWORD="subi@2004"

DB_URL=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

ingestion=DataIngestion(DB_URL)
cleaner=DataCleaning()
ai_agent=AIAgent()
df_csv=ingestion.load_csv("sample_dataset.csv")
if df_csv is not None:
    print("\n cleaning csv data")
    df_csv=cleaner.clean_data(df_csv)
    df_csv=ai_agent.process_data(df_csv)
    print("\n ai-cleaned csv data",df_csv)

df_excel=ingestion.load_excel("sample_dataset.xlsx")
if df_excel is not None:
    print("\n cleaning excel data")
    df_csv=cleaner.clean_data(df_excel)
    df_csv=ai_agent.process_data(df_excel)
    print("\n ai-cleaned excel data",df_excel)

df_db=ingestion.load_from_database("select * from mytable")
if df_db is not None:
    print("\n cleaning database data")
    df_csv=cleaner.clean_data(df_db)
    df_csv=ai_agent.process_data(df_db)
    print("\n ai-cleaned  database data",df_db)

API_URL="https://jsonplaceholder.typicode.com/posts"
df_api=ingestion.fetch_from_api(API_URL)

if df_api is not None:
    print("\n cleaning api data ")
df_api=df_api.head(10)
if "body" in df_api.columns:
    df_api["body"]=df_api["body"].apply(lambda x:x[:100]+"..." if isinstance(x,str) else x)

df_api=cleaner.clean_data(df_api)
df_api=ai_agent.process_data(df_api)
print("\n ai cleaned api data",df_api)

