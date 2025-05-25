import streamlit as st
import requests
import pandas as pd
import json
from io import StringIO

FASTAPI_URL="http://127.0.0.1:8000"
st.set_page_config(page_title="ai powered data cleaning",layout="wide")
st.sidebar.header("Data Source Selection")
data_source=st.sidebar.radio(
    "Select data source:",
    ["CSV/Excel","Database Query","API_Data"],
    index=0
)

st.markdown("""
   # **AI powered data cleaning**
    *clean your data effortlessly using AI-powered processing*
""")
if data_source=="CSV/Excel":
    st.subheader("upload file for cleaning")
    uploaded_file=st.file_uploader("choose a csv or excel file",type=["csv","xlsx"])
    if uploaded_file is not None:
        file_extension=uploaded_file.name.split(".")[-1]
        if file_extension=="csv":
            df=pd.read_csv(uploaded_file)
        else:
            df=pd.read_excel(uploaded_file)
        st.write("### Raw data preview")
        st.dataframe(df)
        if st.button("clean data"):
            files={"file":(uploaded_file.name,uploaded_file.getvalue())}
            response=requests.post(f"{FASTAPI_URL}/clean data",files=files)
            if response.status_code==200:
                st.subheader("Raw api response debugging")
                st.json(response.json())

                try:
                    cleaned_data_raw=response.json()["cleaned_data"]
                    if isinstance(cleaned_data_raw,str):
                        cleaned_data=pd.DataFrame(json.loads(cleaned_data_raw))
                    else:
                        cleaned_data=pd.DataFrame(cleaned_data_raw)
                    st.subheader("cleaned data")
                    st.dataframe(cleaned_data)
                except Exception as e:
                    st.error(f"error converting response to Dataframe:{0}")
            else:
                st.error("failed to clean data")

        elif data_source=="Database Query":
            st.subheader("enter database query")
            db_url=st.text_input("Database connection url","postgresql://user:password@localhost:5432/db")
            query=st.text_area("entersql query","select * from mytable;")

            if st.button("fetch and clean data"):
                response=requests.post(f"{FASTAPI_URL}/clean db",json={"db_url":db_url,"query":query})

                if response.status_code==200:
                    st.header("raw api response debugging")
                    st.json(response.json())
                    try:
                         cleaned_data_raw=response.json()["cleaned_data"]
                         if isinstance(cleaned_data_raw,str):
                            cleaned_data=pd.DataFrame(json.loads(cleaned_data_raw))
                         else:
                            cleaned_data=pd.DataFrame(cleaned_data_raw)
                         st.subheader("cleaned data")
                         st.dataframe(cleaned_data)
                    except Exception as e:
                         st.error(f"error converting response to Dataframe:{0}")
            else:
                st.error("failed to fetch/clean data from database")
        elif data_source=="API Data":
            st.subheader("fetch data from api")
            api_url=st.text_input("enter api endpoint","https://jsonplaceholder.typicode.com/posts")

            if st.button("fetch and clean data"):
                response=requests.post(f"{FASTAPI_URL}/clean api",json={"api_url":api_url})
                if response.status_code==200:
                    st.subheader("raw api response debugging")
                    st.json(response.json())
                    try:
                         cleaned_data_raw=response.json()["cleaned_data"]
                         if isinstance(cleaned_data_raw,str):
                            cleaned_data=pd.DataFrame(json.loads(cleaned_data_raw))
                         else:
                            cleaned_data=pd.DataFrame(cleaned_data_raw)
                         st.subheader("cleaned data")
                         st.dataframe(cleaned_data)
                    except Exception as e:
                         st.error(f"error converting response to Dataframe:{0}")
            else:
                 st.error("failed to fetch/clean data from API")

st.markdown("""
   *built with streamlit+FASTAPI+AI for automated data cleaning*
""")
